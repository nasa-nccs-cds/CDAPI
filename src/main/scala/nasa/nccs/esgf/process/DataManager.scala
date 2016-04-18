package nasa.nccs.esgf.process
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.kernels.AxisIndices
import ucar.ma2

import collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.concurrent
import scala.concurrent.Future

object FragmentSelectionCriteria extends Enumeration { val Largest, Smallest = Value }

trait DataLoader {
  def getDataset( collection: String, varName: String ): CDSDataset
  def getVariable( collection: String, varName: String ): CDSVariable
  def getFragment( fragSpec: DataFragmentSpec, abortSizeFraction: Float=0f ): PartitionedFragment
  def findEnclosingFragSpecs( fKeyChild: DataFragmentKey, admitEquality: Boolean = true): Set[DataFragmentKey]
  def findEnclosedFragSpecs( fKeyParent: DataFragmentKey, admitEquality: Boolean = true): Set[DataFragmentKey]
}

trait ScopeContext {
  def getConfiguration: Map[String,String]
  def config( key: String, default: String ): String = getConfiguration.getOrElse(key,default)
  def config( key: String ): Option[String] = getConfiguration.get(key)
}

class RequestContext( val domains: Map[String,DomainContainer], val inputs: Map[String, OperationInputSpec], private val configuration: Map[String,String] ) extends ScopeContext {
  def getConfiguration = configuration
  def missing_variable(uid: String) = throw new Exception("Can't find Variable '%s' in uids: [ %s ]".format(uid, inputs.keySet.mkString(", ")))
  def getDataSources: Map[String, OperationInputSpec] = inputs
  def getInputSpec( uid: String ): OperationInputSpec = inputs.get( uid ) match {
    case Some(inputSpec) => inputSpec
    case None => missing_variable(uid)
  }
  def getAxisIndices( uid: String ): AxisIndices = inputs.get(uid) match {
    case Some(inputSpec) => inputSpec.axes
    case None => missing_variable(uid)
  }
  def getDomain(domain_id: String): DomainContainer = domains.get(domain_id) match {
    case Some(domain_container) => domain_container
    case None => throw new Exception("Undefined domain in ExecutionContext: " + domain_id)
  }
}

case class OperationInputSpec( data: DataFragmentSpec, axes: AxisIndices ) {}

class ServerContext( val dataLoader: DataLoader, private val configuration: Map[String,String] )  extends ScopeContext {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def getConfiguration = configuration

  def getBinnedArrayFactory(operationCx: OperationContext, requestCx: RequestContext): Option[BinnedArrayFactory] = {
    val uid = operationCx.inputs.head
    operationCx.config("bins") match {
      case None => None
      case Some(binSpec) => Option(BinnedArrayFactory(binSpec, getVariable( requestCx.getInputSpec(uid).data ) ) )
    }
  }
  def inputs( inputSpecs: List[OperationInputSpec] ): List[KernelDataInput] = for( inputSpec <- inputSpecs ) yield new KernelDataInput( getVariableData(inputSpec.data), inputSpec.axes )
  def getVariable(fragSpec: DataFragmentSpec ): CDSVariable = dataLoader.getVariable( fragSpec.collection, fragSpec.varname )
  def getVariable(collection: String, varname: String ): CDSVariable = dataLoader.getVariable( collection, varname )
  def getVariableData( fragSpec: DataFragmentSpec ): PartitionedFragment = dataLoader.getFragment( fragSpec )

  def getAxisData( fragSpec: DataFragmentSpec, axis: Char ): ( Int, ma2.Array ) = {
    val variable: CDSVariable = dataLoader.getVariable( fragSpec.collection, fragSpec.varname )
    val coordAxis = variable.dataset.getCoordinateAxis( axis )
    val axisIndex = variable.ncVariable.findDimensionIndex(coordAxis.getShortName)
    val range = fragSpec.roi.getRange( axisIndex )
    ( axisIndex -> coordAxis.read( List(range) ) )
  }

  def getDataset(collection: String, varname: String ): CDSDataset = dataLoader.getDataset( collection, varname )


  def computeAxisSpecs( fragSpec: DataFragmentSpec, axisConf: List[OperationSpecs] ): AxisIndices = {
    val variable: CDSVariable = getVariable(fragSpec)
    variable.getAxisIndices( axisConf )
  }

  def getSubset( fragSpec: DataFragmentSpec, new_domain_container: DomainContainer ): PartitionedFragment = {
    val t0 = System.nanoTime
    val baseFragment = dataLoader.getFragment( fragSpec )
    val t1 = System.nanoTime
    val variable = getVariable( fragSpec )
    val newFragmentSpec = variable.createFragmentSpec( new_domain_container.axes )
    val rv = baseFragment.cutIntersection( newFragmentSpec.roi )
    val t2 = System.nanoTime
    logger.info( " GetSubsetT: %.4f %.4f".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
    rv
  }


//
//  def getSubset(uid: String, domain_container: DomainContainer): PartitionedFragment = {
//    uidToSource.get(uid) match {
//      case Some(dataSource) =>
//        dataSource.getData match {
//          case None => throw new Exception("Can't find data fragment for data source:  %s " + dataSource.toString)
//          case Some(fragment) => fragment.cutIntersection(getVariable(dataSource).getSubSection(domain_container.axes), true)
//        }
//      case None => missing_variable(uid)
//    }
//  }
//


  def loadVariableData( dataContainer: DataContainer, domain_container_opt: Option[DomainContainer] ): (String, OperationInputSpec) = {
    val data_source: DataSource = dataContainer.getSource
    val t0 = System.nanoTime
    val variable: CDSVariable = dataLoader.getVariable(data_source.collection, data_source.name)
    val t1 = System.nanoTime
    val axisSpecs: AxisIndices = variable.getAxisIndices( dataContainer.getOpSpecs )
    val t2 = System.nanoTime
    val fragmentSpec = domain_container_opt match {
      case Some(domain_container) =>
        val fragmentSpec: DataFragmentSpec = variable.createFragmentSpec(domain_container.axes)
        dataLoader.getFragment( fragmentSpec, 0.3f )
        fragmentSpec
      case None => variable.createFragmentSpec()
    }
    val t3 = System.nanoTime
    logger.info( " loadVariableDataT: %.4f %.4f ".format( (t1-t0)/1.0E9, (t3-t2)/1.0E9 ) )
    return ( dataContainer.uid -> new OperationInputSpec( fragmentSpec, axisSpecs )  )
  }
}


