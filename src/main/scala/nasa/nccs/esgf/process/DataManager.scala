package nasa.nccs.esgf.process

import nasa.nccs.cdapi.cdm.{BinnedArrayFactory, CDSVariable, PartitionedFragment, CDSDataset}
import collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.concurrent

trait DataLoader {
  def getDataset( data_source: DataSource ): CDSDataset
}

class DataManager( val dataLoader: DataLoader ) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.engine.DataManager")
  private val uidToSource = mutable.HashMap[String,DataSource]()

  def getDataSources: Map[String,DataSource] = uidToSource.toMap

  def getBinnedArrayFactory(operation: OperationContainer): Option[BinnedArrayFactory] = {
    val uid = operation.inputs(0)
    operation.optargs.get("bins") match {
      case None => None
      case Some(binSpec) => Option( BinnedArrayFactory(binSpec, getVariable(uid).dataset) )
    }
  }
  def missing_variable( uid: String ) = { throw new Exception( "Can't find Variable '%s' in uids: [ %s ]".format( uid, uidToSource.keySet.mkString(", "))) }

  def getVariable(uid: String): CDSVariable = {
    uidToSource.get(uid) match {
      case Some(dataSource) => getVariable( dataSource )
      case None => missing_variable(uid)
    }
  }
  def getVariable( dataSource: DataSource ): CDSVariable = {
    var dataset: CDSDataset = dataLoader.getDataset( dataSource )
    dataset.loadVariable( dataSource.name )
  }

  def getVariableData(uid: String): PartitionedFragment = {
    uidToSource.get(uid) match {
      case Some(dataSource) =>
        dataSource.getData match {
          case None => throw new Exception( "Can't find data fragment for data source:  %s " + dataSource.toString )
          case Some( fragment ) => fragment
        }
      case None => missing_variable(uid)
    }
  }

  def getSubset( uid: String, domain_container: DomainContainer ): PartitionedFragment = {
    uidToSource.get(uid) match {
      case Some(dataSource) =>
        dataSource.getData match {
          case None => throw new Exception( "Can't find data fragment for data source:  %s " + dataSource.toString )
          case Some( fragment ) =>  fragment.cutIntersection( getVariable( dataSource ).getSubSection( domain_container.axes ), true )
        }
      case None => missing_variable(uid)
    }
  }

  def loadVariableData( dataContainer: DataContainer, domain_container: DomainContainer ): PartitionedFragment = {
    val uid = dataContainer.uid
    val data_source = dataContainer.getSource
    data_source.getData match {
      case None =>
        val dataset: CDSDataset = dataLoader.getDataset(data_source)
        val variable = dataset.loadVariable(data_source.name)
        val fragment = variable.loadRoi(domain_container.axes, dataContainer.getOpSpecs)
        uidToSource += (uid -> data_source)
        data_source.setData(fragment)
        logger.info("Loaded variable %s (%s:%s) subset data, shape = %s ".format(uid, data_source.collection, data_source.name, fragment.shape.toString))
        fragment
      case Some(fragment) => fragment
    }
  }
}
