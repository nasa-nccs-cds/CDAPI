package nasa.nccs.cdapi.kernels

import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor
import nasa.nccs.cdapi.cdm._
import nasa.nccs.esgf.process._
import org.slf4j.LoggerFactory
import java.io.{File, IOException}

import ucar.{ma2, nc2}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object Port {
  def apply( name: String, cardinality: String, description: String="", datatype: String="", identifier: String="" ) = {
    new Port(  name,  cardinality,  description, datatype,  identifier )
  }
}

class Port( val name: String, val cardinality: String, val description: String, val datatype: String, val identifier: String )  {

  def toXml = {
    <port name={name} cardinality={cardinality}>
      { if ( description.nonEmpty ) <description> {description} </description> }
      { if ( datatype.nonEmpty ) <datatype> {datatype} </datatype> }
      { if ( identifier.nonEmpty ) <identifier> {identifier} </identifier> }
    </port>
  }
}

trait ExecutionResult {
  def toXml: xml.Elem
}

class BlockingExecutionResult( val id: String, val intputSpecs: List[DataFragmentSpec], val gridSpec: GridSpec, val result_data: Array[Float] ) extends ExecutionResult {
  def toXml = <result> { result_data.mkString( " ", ",", " " ) } </result>  // cdsutils.cdata(
}

class AsyncExecutionResult( val results: List[String] )  extends ExecutionResult  {
  def this( resultOpt: Option[String]  ) { this( resultOpt.toList ) }
  def toXml = <result> {  results.mkString(",")  } </result>
}

class ExecutionResults( val results: List[ExecutionResult] ) {
  def toXml = <results> { results.map(_.toXml) } </results>
}

case class ResultManifest( val name: String, val dataset: String, val description: String, val units: String )

//class SingleInputExecutionResult( val operation: String, manifest: ResultManifest, result_data: Array[Float] ) extends ExecutionResult(result_data) {
//  val name = manifest.name
//  val description = manifest.description
//  val units = manifest.units
//  val dataset =  manifest.dataset
//
//  override def toXml =
//    <operation id={ operation }>
//      <input name={ name } dataset={ dataset } units={ units } description={ description }  />
//      { super.toXml }
//    </operation>
//}

abstract class DataFragment( private val array: Nd4jMaskedTensor )  extends Serializable {
  val metaData = new mutable.HashMap[String, String]

  def this( array: Nd4jMaskedTensor, metaDataVar: (String, String)* ) {
    this( array )
    metaDataVar.map(p => metaData += p)
  }
  def data: Nd4jMaskedTensor = array
  def name = array.name
  def shape: List[Int] = array.shape.toList
}


class AxisIndices( private val axisIds: Set[Int] = Set.empty ) {
  def getAxes: Seq[Int] = axisIds.toSeq
}

class ExecutionContext( val id: String, val fragments: List[KernelDataInput], val binArrayOpt: Option[BinnedArrayFactory], val domainMap: Map[String,DomainContainer], val dataManager: DataManager, val serverConfiguration: Map[String, String], val args: Map[String, String] ) {

  def getDomain( domain_id: String ): DomainContainer= {
    domainMap.get(domain_id) match {
      case Some(domain_container) => domain_container
      case None =>
        throw new Exception("Undefined domain in ExecutionContext: " + domain_id)
    }
  }
//  def getSubset( var_uid: String, domain_id: String ) = {
//    dataManager.getSubset( var_uid, getDomain(domain_id) )
//  }
  def getDataSources: Map[String,OperationInputSpec] = dataManager.getDataSources

  def async: Boolean = args.getOrElse("async", "false").toBoolean

  def getFragmentSpec( uid: String ): DataFragmentSpec = dataManager.getOperationInputSpec(uid) match {
    case None => throw new Exception( "Missing Data Fragment Spec: " + uid )
    case Some( inputSpec ) => inputSpec.data
  }

  def getAxisIndices( uid: String ): AxisIndices = dataManager.getAxisIndices( uid )
}

object Kernel {
  def getResultFile( serverConfiguration: Map[String,String], resultId: String, deleteExisting: Boolean = false ): File = {
    val resultsDirPath = serverConfiguration.getOrElse("wps.results.dir", System.getProperty("user.home") + "/.wps/results")
    val resultsDir = new File(resultsDirPath); resultsDir.mkdirs()
    val resultFile = new File( resultsDirPath + s"/$resultId.nc" )
    if( deleteExisting && resultFile.exists ) resultFile.delete
    resultFile
  }
}

abstract class Kernel {
  val logger = LoggerFactory.getLogger(this.getClass)
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  def operation: String = identifiers.last.toLowerCase
  def module = identifiers.dropRight(1).mkString(".")
  def id   = identifiers.mkString(".")
  def name = identifiers.takeRight(2).mkString(".")

  val inputs: List[Port]
  val outputs: List[Port]
  val description: String = ""
  val keywords: List[String] = List()
  val identifier: String = ""
  val metadata: String = ""

  def execute( context: ExecutionContext ): ExecutionResult
  def toXmlHeader =  <kernel module={module} name={name}> { if (description.nonEmpty) <description> {description} </description> } </kernel>

  def toXml = {
    <kernel module={module} name={name}>
      {if (description.nonEmpty) <description>{description}</description> }
      {if (keywords.nonEmpty) <keywords> {keywords.mkString(",")} </keywords> }
      {if (identifier.nonEmpty) <identifier> {identifier} </identifier> }
      {if (metadata.nonEmpty) <metadata> {metadata} </metadata> }
    </kernel>
  }

  def searchForValue( metadata: Map[String,nc2.Attribute], keys: List[String], default_val: String ) : String = {
    keys.length match {
      case 0 => default_val
      case x => metadata.get(keys.head) match {
        case Some(valueAttr) => valueAttr.getStringValue()
        case None => searchForValue(metadata, keys.tail, default_val)
      }
    }
  }

  def saveResult( maskedTensor: Nd4jMaskedTensor, context: ExecutionContext, varMetadata: Map[String,nc2.Attribute], dsetMetadata: List[nc2.Attribute] ): Option[String] = {
    varMetadata.get("axes") match {
      case None => logger.error("Can't write NetCDF data without axis information")
      case Some(axisAttr) =>
        val axes = axisAttr.getStringValue(0).split(' ')
        context.args.get("resultId") match {
          case None => logger.warn("Missing resultId: can't save result")
          case Some(resultId) =>
            val varname = searchForValue( varMetadata, List("varname","fullname","standard_name","original_name","long_name"), "Nd4jMaskedTensor" )
            val resultFile = Kernel.getResultFile( context.serverConfiguration, resultId, true )
            val writer: nc2.NetcdfFileWriter = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, resultFile.getAbsolutePath )
            assert(axes.length == maskedTensor.shape.length, "Axes not the same length as data shape in saveResult")
            val dims: IndexedSeq[nc2.Dimension] = for (idim <- (0 until axes.length); axis = axes(idim); size = maskedTensor.shape(idim)) yield writer.addDimension(null, axis, size)
            val variable: nc2.Variable = writer.addVariable(null, varname, ma2.DataType.FLOAT, dims.toList)
            varMetadata.values.foreach( attr => variable.addAttribute(attr) )
            variable.addAttribute( new nc2.Attribute( "missing_value", maskedTensor.invalid ) )
            dsetMetadata.foreach( attr => writer.addGroupAttribute(null, attr ) )
            try {
              writer.create()
              writer.write( variable, maskedTensor.ma2Data )
              writer.close()
              println( "Writing result %s to file '%s'".format(resultId,resultFile.getAbsolutePath) )
              Some(resultId)
            } catch {
              case e: IOException => logger.error("ERROR creating file %s%n%s".format(resultFile.getAbsolutePath, e.getMessage()))
            }
        }
    }
    None
  }
}

class KernelModule {
  val logger = LoggerFactory.getLogger(this.getClass)
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  logger.info( "---> new KernelModule: " + identifiers.mkString(", ") )
  def package_path = identifiers.dropRight(1).mkString(".")
  def name: String = identifiers.last
  val version = ""
  val organization = ""
  val author = ""
  val contact = ""
  val kernelMap: Map[String,Kernel] = Map(getKernelObjects.map( kernel => kernel.operation -> kernel ): _*)

  def getKernelClasses = getInnerClasses.filter( _.getSuperclass.getName.split('.').last == "Kernel"  )
  def getInnerClasses = this.getClass.getClasses.toList
  def getKernelObjects = getKernelClasses.map( _.getDeclaredConstructors()(0).newInstance(this).asInstanceOf[Kernel] )

  def getKernel( kernelName: String ): Option[Kernel] = kernelMap.get( kernelName )
  def getKernelNames: List[String] = kernelMap.keys.toList

  def toXml = {
    <kernelModule name={name}>
      { if ( version.nonEmpty ) <version> {version} </version> }
      { if ( organization.nonEmpty ) <organization> {organization} </organization> }
      { if ( author.nonEmpty ) <author> {author} </author> }
      { if ( contact.nonEmpty ) <contact> {contact} </contact> }
      <kernels> { kernelMap.values.map( _.toXmlHeader ) } </kernels>
    </kernelModule>
  }
}

