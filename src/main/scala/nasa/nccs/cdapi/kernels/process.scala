package nasa.nccs.cdapi.kernels
import nasa.nccs.cdapi.tensors.AbstractTensor
import org.slf4j.LoggerFactory

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

class ExecutionResult( val result_data: Array[Float] ) {
  def toXml = <result> { result_data.mkString( " ", ",", " " ) } </result>  // cdsutils.cdata(
}

class ExecutionResults( val results: List[ExecutionResult] ) {
  def toXml = <execution> {  results.map(_.toXml )  } </execution>
}

case class ResultManifest( val name: String, val dataset: String, val description: String, val units: String )

class SingleInputExecutionResult( val operation: String, manifest: ResultManifest, result_data: Array[Float] ) extends ExecutionResult(result_data) {
  val name = manifest.name
  val description = manifest.description
  val units = manifest.units
  val dataset =  manifest.dataset

  override def toXml =
    <operation id={ operation }>
      <input name={ name } dataset={ dataset } units={ units } description={ description }  />
      { super.toXml }
    </operation>
}

abstract class DataFragment( array: AbstractTensor )  extends Serializable {
  val metaData = new mutable.HashMap[String, String]

  def this( array: AbstractTensor, metaDataVar: (String, String)* ) {
    this( array )
    metaDataVar.map(p => metaData += p)
  }

  def data: AbstractTensor = array
  def name = array.name
  def shape: List[Int] = array.shape.toList

}

abstract class Kernel {
  val logger = LoggerFactory.getLogger(this.getClass)
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  def operation: String = identifiers.last
  def module = identifiers.dropRight(1).mkString(".")
  def id   = identifiers.mkString(".")
  def name = identifiers.takeRight(2).mkString(".")

  val inputs: List[Port]
  val outputs: List[Port]
  val description: String = ""
  val keywords: List[String] = List()
  val identifier: String = ""
  val metadata: String = ""

  def execute(inputSubsets: List[DataFragment], run_args: Map[String, Any]): ExecutionResult
  def toXmlHeader =  <kernel module={module} name={name}> { if (description.nonEmpty) <description> {description} </description> } </kernel>

  def toXml = {
    <kernel module={module} name={name}>
      {if (description.nonEmpty) <description>{description}</description> }
      {if (keywords.nonEmpty) <keywords> {keywords.mkString(",")} </keywords> }
      {if (identifier.nonEmpty) <identifier> {identifier} </identifier> }
      {if (metadata.nonEmpty) <metadata> {metadata} </metadata> }
    </kernel>
  }
}

class KernelModule {
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  def package_path = identifiers.dropRight(1).mkString(".")
  def name: String = identifiers.last
  val version = ""
  val organization = ""
  val author = ""
  val contact = ""
  val kernelMap: Map[String,Kernel] = Map(getKernelObjects.map( kernel => kernel.name -> kernel ): _*)

  def getKernelClasses = getInnerClasses.filter( _.getSuperclass.getName.split('.').last == "Kernel"  )
  def getInnerClasses = this.getClass.getClasses.toList
  def getKernelObjects = getKernelClasses.map( _.getDeclaredConstructors()(0).newInstance(this).asInstanceOf[Kernel] )

  def getKernel( kernelName: String ): Option[Kernel] = kernelMap.get( kernelName )

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

