package nasa.nccs.esgf.process

import nasa.nccs.cdapi.cdm.{BinnedArrayFactory, CDSVariable, PartitionedFragment, CDSDataset}
import nasa.nccs.cdapi.kernels.DataFragment
import ucar.ma2

import scala.collection.mutable

trait DataLoader {
  def getDataset( data_source: DataSource ): CDSDataset
}

class DataManager( val domainMap: Map[String,DomainContainer], val dataLoader: DataLoader ) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.engine.DataManager")
  var subsets = mutable.Map[String,PartitionedFragment]()
  var variables = mutable.Map[String,CDSVariable]()

  def getBinnedArrayFactory( operation: OperationContainer ): Option[BinnedArrayFactory] = {
    val uid = operation.inputs(0)
    operation.optargs.get("bins") match {
      case None => None
      case Some(binSpec) =>
        variables.get(uid) match {
          case None => throw new Exception( "DataManager can't find variable %s in getBinScaffold, variables = [%s]".format(uid,variables.keys.mkString(",")))
          case Some(variable) => Some( BinnedArrayFactory( binSpec, variable.dataset ) )
        }
    }
  }

  def getVariableData(uid: String): PartitionedFragment = {
    subsets.get(uid) match {
      case Some(subset) => subset
      case None => throw new Exception("Can't find subset Data for Variable $uid")
    }
  }

  def getSubset( uid: String, domain: String ): PartitionedFragment = {
    subsets.get( uid ) match {
      case None => throw new Exception("Can't find variable data for uid '" + uid + "' in getSubset" )
      case Some(fragment) => domainMap.get(domain) match {
        case None => throw new Exception("Undefined domain for fragment " + uid + ", domain = " + domain)
        case Some(domain_container) => variables.get( uid ) match {
          case None => throw new Exception("Undefined variable for fragment " + uid + ", domain = " + domain)
          case Some(variable) => fragment.cutIntersection( variable.getSubSection( domain_container.axes ), true )
        }
      }
    }
  }

  def loadVariableData( dataContainer: DataContainer ): DataFragment = {
    val uid = dataContainer.uid
    val data_source = dataContainer.getSource
    subsets.get(uid) match {
      case Some(subset) => subset
      case None =>
        val dataset: CDSDataset = dataLoader.getDataset(data_source)
        domainMap.get(data_source.domain) match {
          case Some(domain_container) =>
            val variable = dataset.loadVariable( uid, data_source.name )
            val fragment = variable.loadRoi( domain_container.axes, dataContainer.getOpSpecs )
            subsets += uid -> fragment
            variables += uid -> variable
            logger.info("Loaded variable %s (%s:%s) subset data, shape = %s ".format(uid, data_source.collection, data_source.name, fragment.shape.toString) )
            fragment
          case None =>
            throw new Exception("Undefined domain for dataset " + data_source.name + ", domain = " + data_source.domain)
        }
    }
  }

}
