package nasa.nccs.cdapi.cdm

import java.nio.channels.NonReadableChannelException

import nasa.nccs.esgf.process.DomainAxis
import nasa.nccs.cdapi.cdm
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{ NetcdfDataset, CoordinateSystem, CoordinateAxis }
import scala.collection.mutable
import scala.collection.concurrent
import scala.collection.JavaConversions._
// import scala.collection.JavaConverters._

object Collection {
  def apply( ctype: String, url: String, vars: List[String] = List() ) = { new Collection(ctype,url,vars) }
}
class Collection( val ctype: String, val url: String, val vars: List[String] = List() ) {
  def getUri( varName: String = "" ) = {
    ctype match {
      case "dods" => s"$url/$varName.ncml"
      case _ => throw new Exception( s"Unrecognized collection type: $ctype")
    }
  }
}

object CDSDataset {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def load( dsetName: String, collection: Collection, varName: String = "" ) = {
    val uri = collection.getUri( varName )
    val ncDataset: NetcdfDataset = loadNetCDFDataSet( uri )
    val coordSystems: List[CoordinateSystem] = ncDataset.getCoordinateSystems.toList
    assert( coordSystems.size <= 1, "Multiple coordinate systems for one dataset is not supported" )
    if(coordSystems.isEmpty) throw new IllegalStateException("Error creating coordinate system for variable " + varName )
    new CDSDataset( dsetName, uri, ncDataset, coordSystems.head )
  }

  private def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    logger.info("Opening NetCDF dataset %s".format(url))
    try {
      NetcdfDataset.openDataset(url)
    } catch {
      case e: java.io.IOException =>
        logger.error("Couldn't open dataset %s".format(url))
        throw e
      case ex: Exception =>
        logger.error("Something went wrong while reading %s".format(url))
        throw ex
    }
  }
}

class CDSDataset( val name: String, val uri: String, val ncDataset: NetcdfDataset, val coordSystem: CoordinateSystem ) {
  val attributes = ncDataset.getGlobalAttributes
  val coordAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList
  val variables = concurrent.TrieMap[String,cdm.CDSVariable]()

  def getCoordinateAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList

  def loadVariable( varName: String ): cdm.CDSVariable = {
    variables.get(varName) match {
      case None =>
        val ncVariable = ncDataset.findVariable(varName)
        if (ncVariable == null) throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
        val cdsVariable = new cdm.CDSVariable( varName, this, ncVariable )
        variables += ( varName -> cdsVariable )
        cdsVariable
      case Some(cdsVariable) => cdsVariable
    }
  }
  def getVariable( varName: String ): cdm.CDSVariable = {
    variables.get(varName) match {
      case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
      case Some(cdsVariable) => cdsVariable
    }
  }
  def getCoordinateAxis( axisType: DomainAxis.Type.Value ): Option[CoordinateAxis] = {
    axisType match {
      case DomainAxis.Type.X => Option( coordSystem.getXaxis )
      case DomainAxis.Type.Y => Option( coordSystem.getYaxis )
      case DomainAxis.Type.Z => Option( coordSystem.getHeightAxis )
      case DomainAxis.Type.Lon => Option( coordSystem.getLonAxis )
      case DomainAxis.Type.Lat => Option( coordSystem.getLatAxis )
      case DomainAxis.Type.Lev => Option( coordSystem.getPressureAxis )
      case DomainAxis.Type.T => Option( coordSystem.getTaxis )
    }
  }

  def getCoordinateAxis(axisType: Char): CoordinateAxis = {
    axisType match {
      case 'x' => if (coordSystem.isGeoXY) coordSystem.getXaxis else coordSystem.getLonAxis
      case 'y' => if (coordSystem.isGeoXY) coordSystem.getYaxis else coordSystem.getLatAxis
      case 'z' =>
        if (coordSystem.containsAxisType(AxisType.Pressure)) coordSystem.getPressureAxis
        else if (coordSystem.containsAxisType(AxisType.Height)) coordSystem.getHeightAxis else coordSystem.getZaxis
      case 't' => coordSystem.getTaxis
      case x => throw new Exception("Can't recognize axis type '%c'".format(x))
    }
  }
}

// var.findDimensionIndex(java.lang.String name)
