package nasa.nccs.esgf.process
import nasa.nccs.cdapi.cdm.{BinnedArrayFactory, CDSVariable, PartitionedFragment, CDSDataset}
import nasa.nccs.cdapi.kernels.AxisSpecs
import collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.concurrent

trait DataLoader {
  def getDataset( collection: String, varName: String ): CDSDataset
  def getVariable( collection: String, varName: String ): CDSVariable
  def getFragment( fragSpec: DataFragmentSpec ): PartitionedFragment
  def findEnclosingFragment(targetFragSpec: DataFragmentSpec): Option[DataFragmentSpec]
}

class DataManager( val dataLoader: DataLoader ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  private val uidToSource = mutable.HashMap[String, DataFragmentSpec]()

  def getDataSources: Map[String, DataFragmentSpec] = uidToSource.toMap
  def getFragmentSpec( uid: String ): Option[DataFragmentSpec] = uidToSource.get( uid )

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
      case Some(fragSpec) => dataLoader.getVariable( fragSpec.collection, fragSpec.varname )
      case None => missing_variable(uid)
    }
  }

  def getAxisSpecs( uid: String, axisConf: List[OperationSpecs] ): AxisSpecs = {
    val variable: CDSVariable = getVariable(uid)
    variable.getAxisSpecs( axisConf )
  }

  def getSubset( var_uid: String, baseFragmentSpec: DataFragmentSpec, new_domain_container: DomainContainer ): PartitionedFragment = {
    val baseFragment = dataLoader.getFragment(baseFragmentSpec)
    val variable = getVariable( var_uid )
    val newFragmentSpec = variable.createFragmentSpec( new_domain_container.axes )
    baseFragment.cutNewSubset( newFragmentSpec.roi, true )
  }

  def getVariableData( uid: String ): PartitionedFragment = {
    uidToSource.get(uid) match {
      case Some(fragSpec) => dataLoader.getFragment( fragSpec )
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

  def loadVariableData(dataContainer: DataContainer, domain_container: DomainContainer): PartitionedFragment = {
    loadVariableData(dataContainer.uid, dataContainer.getSource, domain_container.axes)
  }

  def loadVariableData( uid: String, data_source: DataSource, axes: List[DomainAxis]): PartitionedFragment = {
    val dataset: CDSDataset = dataLoader.getDataset(data_source.collection, data_source.name)
    val variable = dataset.loadVariable(data_source.name)
    val fragmentSpec: DataFragmentSpec = variable.createFragmentSpec(axes)
    uidToSource += ( uid -> fragmentSpec )
    dataLoader.getFragment(fragmentSpec)
  }
}
