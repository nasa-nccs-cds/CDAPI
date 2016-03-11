package nasa.nccs.esgf.process
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.kernels.AxisSpecs
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
  def getFragment( fragSpec: DataFragmentSpec ): PartitionedFragment
  def findEnclosingFragSpecs(targetFragSpec: DataFragmentSpec): Set[DataFragmentSpec]
  def findEnclosedFragSpecs(targetFragSpec: DataFragmentSpec): Set[DataFragmentSpec]
}

case class OperationInputSpec( data: DataFragmentSpec, axes: AxisSpecs ) {}

class DataManager( val dataLoader: DataLoader ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  private val uidToSource = TrieMap[String, OperationInputSpec]()

  def getDataSources: Map[String, OperationInputSpec] = uidToSource.toMap
  def getOperationInputSpec( uid: String ): Option[OperationInputSpec] = uidToSource.get( uid )

  def getAxisSpecs( uid: String ): AxisSpecs = uidToSource.get(uid) match {
    case Some(inputSpec) => inputSpec.axes
    case None => missing_variable(uid)
  }

  def getBinnedArrayFactory(operation: OperationContainer): Option[BinnedArrayFactory] = {
    val uid = operation.inputs(0)
    operation.optargs.get("bins") match {
      case None => None
      case Some(binSpec) => Option(BinnedArrayFactory(binSpec, getVariable(uid).dataset))
    }
  }

  def missing_variable(uid: String) = {
    throw new Exception("Can't find Variable '%s' in uids: [ %s ]".format(uid, uidToSource.keySet.mkString(", ")))
  }

  def getVariable(uid: String): CDSVariable = {
    uidToSource.get(uid) match {
      case Some(inputSpec) => dataLoader.getVariable( inputSpec.data.collection, inputSpec.data.varname )
      case None => missing_variable(uid)
    }
  }

  def computeAxisSpecs( uid: String, axisConf: List[OperationSpecs] ): AxisSpecs = {
    val variable: CDSVariable = getVariable(uid)
    variable.getAxisSpecs( axisConf )
  }

  def getSubset( var_uid: String, baseFragmentSpec: DataFragmentSpec, new_domain_container: DomainContainer ): PartitionedFragment = {
    val baseFragment = dataLoader.getFragment(baseFragmentSpec)
    val variable = getVariable( var_uid )
    val newFragmentSpec = variable.createFragmentSpec( new_domain_container.axes )
    baseFragment.cutIntersection( newFragmentSpec.roi )
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
    val variable = dataLoader.getVariable(data_source.collection, data_source.name)
    val axisSpecs: AxisSpecs = variable.getAxisSpecs( dataContainer.getOpSpecs )
    val fragmentSpec: DataFragmentSpec = variable.createFragmentSpec(domain_container.axes)
    uidToSource += ( dataContainer.uid -> new OperationInputSpec( fragmentSpec, axisSpecs )  )
    dataLoader.getFragment(fragmentSpec)
  }
}


