package nasa.nccs.cdapi.kernels

import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor
import nasa.nccs.cdapi.cdm._
import nasa.nccs.esgf.process._
import org.slf4j.LoggerFactory
import java.io.File
import ucar.nc2.NetcdfFileWriter

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

class BlockingExecutionResult( val result_data: Array[Float] ) extends ExecutionResult {
  def toXml = <result> { result_data.mkString( " ", ",", " " ) } </result>  // cdsutils.cdata(
}

class AsyncExecutionResult( val results: List[String] )  extends ExecutionResult  {
  def this( result: String  ) { this( List(result) ) }
  def toXml = <result> {  results.mkString(",")  } </result>
}

class ExecutionResults( val results: List[ExecutionResult] ) {
  def toXml = <results> { results.foreach(_.toXml) } </results>
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

abstract class DataFragment( array: Nd4jMaskedTensor )  extends Serializable {
  val metaData = new mutable.HashMap[String, String]

  def this( array: Nd4jMaskedTensor, metaDataVar: (String, String)* ) {
    this( array )
    metaDataVar.map(p => metaData += p)
  }
  def data: Nd4jMaskedTensor = array
  def name = array.name
  def shape: List[Int] = array.shape.toList
}


class AxisSpecs( private val axisIds: Set[Int] = Set.empty ) {
  def getAxes: Seq[Int] = axisIds.toSeq
}

class ExecutionContext( val fragments: List[KernelDataInput], val binArrayOpt: Option[BinnedArrayFactory], val domainMap: Map[String,DomainContainer], val dataManager: DataManager, val serverConfiguration: Map[String, String], val args: Map[String, String] ) {

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

  def getAxisSpecs( uid: String ): AxisSpecs = dataManager.getAxisSpecs( uid )
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

  def saveResult( data: Nd4jMaskedTensor, context: ExecutionContext): String = {
    ""
  }
  def saveResult( data: PartitionedFragment, context: ExecutionContext ): String = {
    context.args.get("resultId") match {
      case None => logger.warn( "Missing resultId: can't save result")
      case Some(resultId) =>
        val resultsDirPath = context.serverConfiguration.getOrElse( "wps.results.dir", "~/.wps/results" )
        val resultsDir = new File( resultsDirPath )
        resultsDir.mkdirs()
        resultsDir.toPath()

 //       var fileWriter  = new NetcdfFileWriter()

    }
    ""
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

