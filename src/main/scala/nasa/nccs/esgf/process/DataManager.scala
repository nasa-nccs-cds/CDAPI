package nasa.nccs.esgf.process
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.kernels.AxisIndices
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

case class OperationInputSpec( data: DataFragmentSpec, axes: AxisIndices ) {}

class DataManager( val dataLoader: DataLoader ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  private val uidToSource = TrieMap[String, OperationInputSpec]()

  def getDataSources: Map[String, OperationInputSpec] = uidToSource.toMap
  def getOperationInputSpec( uid: String ): Option[OperationInputSpec] = uidToSource.get( uid )

  def getAxisIndices( uid: String ): AxisIndices = uidToSource.get(uid) match {
    case Some(inputSpec) => inputSpec.axes
    case None => missing_variable(uid)
  }

  def getBinnedArrayFactory(operation: OperationContainer): Option[BinnedArrayFactory] = {
    val uid = operation.inputs.head
    operation.getConfiguration("operation").get("bins") match {
      case None => None
      case Some(binSpec) => Option(BinnedArrayFactory(binSpec, getVariable(uid) ))
    }
  }

  def missing_variable(uid: String) = {
    throw new Exception("Can't find Variable '%s' in uids: [ %s ]".format(uid, uidToSource.keySet.mkString(", ")))
  }

  def getVariable(fragSpec: DataFragmentSpec ): CDSVariable = dataLoader.getVariable( fragSpec.collection, fragSpec.varname )
  def getVariable(collection: String, varname: String ): CDSVariable = dataLoader.getVariable( collection, varname )

  def getDataset(collection: String, varname: String ): CDSDataset = dataLoader.getDataset( collection, varname )

  def getVariable(uid: String): CDSVariable = {
    uidToSource.get(uid) match {
      case Some(inputSpec) => getVariable( inputSpec.data.collection, inputSpec.data.varname )
      case None => missing_variable(uid)
    }
  }

  def computeAxisSpecs( uid: String, axisConf: List[OperationSpecs] ): AxisIndices = {
    val variable: CDSVariable = getVariable(uid)
    variable.getAxisIndices( axisConf )
  }

  def getSubset( var_uid: String, baseFragmentSpec: DataFragmentSpec, new_domain_container: DomainContainer ): PartitionedFragment = {
    val t0 = System.nanoTime
    val baseFragment = dataLoader.getFragment( baseFragmentSpec )
    val t1 = System.nanoTime
    val variable = getVariable( var_uid )
    val newFragmentSpec = variable.createFragmentSpec( new_domain_container.axes )
    val rv = baseFragment.cutIntersection( newFragmentSpec.roi )
    val t2 = System.nanoTime
    logger.info( " GetSubsetT: %.4f %.4f".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
    rv
  }

  def getVariableData( uid: String ): PartitionedFragment = {
    uidToSource.get(uid) match {
      case Some(inputSpec) => dataLoader.getFragment( inputSpec.data )
      case None => missing_variable(uid)
    }
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


  def loadVariableData( dataContainer: DataContainer, domain_container: DomainContainer ): PartitionedFragment = {
    val data_source: DataSource = dataContainer.getSource
    val t0 = System.nanoTime
    val variable = dataLoader.getVariable(data_source.collection, data_source.name)
    val t1 = System.nanoTime
    val axisSpecs: AxisIndices = variable.getAxisIndices( dataContainer.getOpSpecs )
    val fragmentSpec: DataFragmentSpec = variable.createFragmentSpec(domain_container.axes)
    uidToSource += ( dataContainer.uid -> new OperationInputSpec( fragmentSpec, axisSpecs )  )
    val t2 = System.nanoTime
    val rv = dataLoader.getFragment( fragmentSpec, 0.3f )
    val t3 = System.nanoTime
    logger.info( " loadVariableDataT: %.4f %.4f ".format( (t1-t0)/1.0E9, (t3-t2)/1.0E9 ) )
    rv
  }
}


