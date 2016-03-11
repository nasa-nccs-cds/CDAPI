package nasa.nccs.cdapi.cdm

import nasa.nccs.cdapi.kernels.{AxisSpecs, DataFragment}
import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import nasa.nccs.utilities.cdsutils
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.indexing.{INDArrayIndex, NDArrayIndex}
import ucar.nc2.time.{CalendarDate, CalendarDateRange}
import nasa.nccs.esgf.process._
import ucar.{ma2, nc2}
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D, CoordinateSystem, CoordinateAxis1DTime}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.JavaConversions._
// import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.concurrent

object BoundsRole extends Enumeration { val Start, End = Value }

class KernelDataInput( val dataFragment: PartitionedFragment, val axisSpecs: AxisSpecs ) {}

object CDSVariable { }

class CDSVariable( val name: String, val dataset: CDSDataset, val ncVariable: nc2.Variable) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.cdm.CDSVariable")
  val description = ncVariable.getDescription
  val dims = ncVariable.getDimensionsAll.toList
  val units = ncVariable.getUnitsString
  val shape = ncVariable.getShape.toList
  val fullname = ncVariable.getFullName
  val attributes = nc2.Attribute.makeMap(ncVariable.getAttributes).toMap
  val missing = getAttributeValue( "missing_value", "" ) match { case "" => Float.MaxValue; case s => s.toFloat }

  def getAttributeValue( key: String, default_value: String  ) =  attributes.get( key ) match { case Some( attr_val ) => attr_val.toString.split('=').last; case None => default_value }
  override def toString = "\nCDSVariable(%s) { description: '%s', shape: %s, dims: %s, }\n  --> Variable Attributes: %s".format(name, description, shape.mkString("[", " ", "]"), dims.mkString("[", ",", "]"), attributes.mkString("\n\t\t", "\n\t\t", "\n"))
  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase

  def getBoundedCalDate(coordAxis1DTime: CoordinateAxis1DTime, caldate: CalendarDate, role: BoundsRole.Value, strict: Boolean = true): CalendarDate = {
    val date_range: CalendarDateRange = coordAxis1DTime.getCalendarDateRange
    if (!date_range.includes(caldate)) {
      if (strict) throw new IllegalStateException("CDS2-CDSVariable: Time value %s outside of time bounds %s".format(caldate.toString, date_range.toString))
      else {
        if (role == BoundsRole.Start) {
          val startDate: CalendarDate = date_range.getStart
          logger.warn("Time value %s outside of time bounds %s, resetting value to range start date %s".format(caldate.toString, date_range.toString, startDate.toString))
          startDate
        } else {
          val endDate: CalendarDate = date_range.getEnd
          logger.warn("Time value %s outside of time bounds %s, resetting value to range end date %s".format(caldate.toString, date_range.toString, endDate.toString))
          endDate
        }
      }
    } else caldate
  }

  def getTimeCoordIndex(coordAxis: CoordinateAxis, tval: String, role: BoundsRole.Value, strict: Boolean = true): Int = {
    val indexVal: Int = coordAxis match {
      case coordAxis1DTime: CoordinateAxis1DTime =>
        val caldate: CalendarDate = cdsutils.dateTimeParser.parse(tval)
        val caldate_bounded: CalendarDate = getBoundedCalDate(coordAxis1DTime, caldate, role, strict)
        coordAxis1DTime.findTimeIndexFromCalendarDate(caldate_bounded)
      case _ => throw new IllegalStateException("CDS2-CDSVariable: Can't process time axis type: " + coordAxis.getClass.getName)
    }
    indexVal
  }

  def getTimeIndexBounds(coordAxis: CoordinateAxis, startval: String, endval: String, strict: Boolean = true): ma2.Range = {
    val startIndex = getTimeCoordIndex(coordAxis, startval, BoundsRole.Start, strict)
    val endIndex = getTimeCoordIndex(coordAxis, endval, BoundsRole.End, strict)
    new ma2.Range(startIndex, endIndex)
  }

  def getGridCoordIndex(coordAxis: CoordinateAxis, cval: Double, role: BoundsRole.Value, strict: Boolean = true): Int = {
    coordAxis match {
      case coordAxis1D: CoordinateAxis1D =>
        coordAxis1D.findCoordElement(cval) match {
          case -1 =>
            if (role == BoundsRole.Start) {
              val grid_start = coordAxis1D.getCoordValue(0)
              logger.warn("Axis %s: ROI Start value %f outside of grid area, resetting to grid start: %f".format(coordAxis.getShortName, cval, grid_start))
              0
            } else {
              val end_index = coordAxis1D.getSize.toInt - 1
              val grid_end = coordAxis1D.getCoordValue(end_index)
              logger.warn("Axis %s: ROI Start value %s outside of grid area, resetting to grid start: %f".format(coordAxis.getShortName, cval, grid_end))
              end_index
            }
          case ival => ival
        }
      case _ => throw new IllegalStateException("CDS2-CDSVariable: Can't process xyz coord axis type: " + coordAxis.getClass.getName)
    }
  }

  def getGridIndexBounds(coordAxis: CoordinateAxis, startval: Double, endval: Double, strict: Boolean = true): ma2.Range = {
    val startIndex = getGridCoordIndex(coordAxis, startval, BoundsRole.Start, strict)
    val endIndex = getGridCoordIndex(coordAxis, endval, BoundsRole.End, strict)
    new ma2.Range(startIndex, endIndex)
  }

  def getIndexBounds(coordAxis: CoordinateAxis, startval: GenericNumber, endval: GenericNumber, strict: Boolean = true): ma2.Range = {
    val indexRange = if (coordAxis.getAxisType == nc2.constants.AxisType.Time) getTimeIndexBounds(coordAxis, startval.toString, endval.toString ) else getGridIndexBounds(coordAxis, startval, endval)
    assert(indexRange.last >= indexRange.first, "CDS2-CDSVariable: Coordinate bounds appear to be inverted: start = %s, end = %s".format(startval.toString, endval.toString))
    indexRange
  }

  def getSubSection( roi: List[DomainAxis] ): ma2.Section = {
    val shape = ncVariable.getRanges.to[mutable.ArrayBuffer]
    for (axis <- roi) {
      dataset.getCoordinateAxis(axis.axistype) match {
        case Some(coordAxis) =>
          ncVariable.findDimensionIndex(coordAxis.getShortName) match {
            case -1 => throw new IllegalStateException("CDS2-CDSVariable: Can't find axis %s in variable %s".format(coordAxis.getShortName, ncVariable.getNameAndDimensions))
            case dimension_index =>
              axis.system match {
                case "indices" =>
                  shape.update(dimension_index, new ma2.Range(axis.start.toInt, axis.end.toInt, 1))
                case "values" =>
                  val boundedRange = getIndexBounds(coordAxis, axis.start, axis.end)
                  shape.update(dimension_index, boundedRange)
                case _ => throw new IllegalStateException("CDS2-CDSVariable: Illegal system value in axis bounds: " + axis.system)
              }
          }
        case None => logger.warn("Ignoring bounds on %s axis in variable %s".format(axis.name, ncVariable.getNameAndDimensions))
      }
    }
    new ma2.Section( shape )
  }

  def getNDArray( array: ucar.ma2.Array ): INDArray = {
    val t0 = System.nanoTime
    val result = array.getElementType.toString match {
      case "float" =>
        Nd4j.create( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]], array.getShape )
      case "int" =>
        Nd4j.create( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]], array.getShape )
      case "double" =>
        Nd4j.create( array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]], array.getShape )
    }
    val t1 = System.nanoTime
    logger.info( "Converted java array to INDArray, shape = %s, time = %.6f s".format( array.getShape.toList.toString, (t1-t0)/1.0E9 ) )
    result
  }

  def createFragmentSpec( roi: List[DomainAxis], partIndex: Int=0, partAxis: Char='*', nPart: Int=1 ) = {
    val partitions = if ( partAxis == '*' ) List.empty[PartitionSpec] else {
      val partitionAxis = dataset.getCoordinateAxis(partAxis)
      val partAxisIndex = ncVariable.findDimensionIndex(partitionAxis.getShortName)
      assert(partAxisIndex != -1, "CDS2-CDSVariable: Can't find axis %s in variable %s".format(partitionAxis.getShortName, ncVariable.getNameAndDimensions))
      List( new PartitionSpec(partAxisIndex, nPart, partIndex) )
    }
    new DataFragmentSpec( name, dataset.name, getSubSection(roi), partitions:_* )
  }

  def loadPartition( fragmentSpec : DataFragmentSpec, axisConf: List[OperationSpecs] ): PartitionedFragment = {
    val partition = fragmentSpec.partitions.head
    val sp = new SectionPartitioner(fragmentSpec.roi, partition.nPart)
    sp.getPartition(partition.partIndex, partition.axisIndex ) match {
      case Some(partSection) =>
        val array = ncVariable.read(partSection)
        val ndArray: INDArray = getNDArray(array)
        createPartitionedFragment( fragmentSpec, ndArray )
      case None =>
        logger.warn("No fragment generated for partition index %s out of %d parts".format(partition.partIndex, partition.nPart))
        new PartitionedFragment()
    }
  }

  def loadRoi( fragmentSpec: DataFragmentSpec ): PartitionedFragment = {
    val array = ncVariable.read(fragmentSpec.roi)
    val ndArray: INDArray = getNDArray(array)
    createPartitionedFragment( fragmentSpec, ndArray )
  }

  def getAxisSpecs( axisConf: List[OperationSpecs] ): AxisSpecs = {
    val axis_ids = mutable.HashSet[Int]()
    for( opSpec <- axisConf ) {
      val axes = opSpec.getSpec("axes")
      val axis_chars: List[Char] = if( axes.contains(',') ) axes.split(",").map(_.head).toList else axes.toList
      axis_ids ++= axis_chars.map( cval => getAxisIndex( cval ) )
    }
    new AxisSpecs( axisIds=axis_ids.toSet )
  }

  def getAxisIndex( axisClass: Char ): Int = {
    val coord_axis = dataset.getCoordinateAxis(axisClass)
    ncVariable.findDimensionIndex( coord_axis.getShortName )
  }

  def createPartitionedFragment( fragmentSpec: DataFragmentSpec, ndArray: INDArray ): PartitionedFragment =  {
    new PartitionedFragment(new Nd4jMaskedTensor(ndArray, missing), fragmentSpec.roi)
  }

}

object PartitionedFragment {
  def sectionToIndices( section: ma2.Section ): List[INDArrayIndex] = section.getRanges.map(range => NDArrayIndex.interval( range.first, range.last+1 ) ).toList
}

class PartitionedFragment( array: Nd4jMaskedTensor, val roiSection: ma2.Section, metaDataVar: (String, String)*  ) extends DataFragment( array, metaDataVar:_* ) {
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def this() = this( new Nd4jMaskedTensor, new ma2.Section )

  override def toString = { "PartitionedFragment: shape = %s, section = %s".format( array.shape.toString, roiSection.toString ) }

  def cutIntersection( cutSection: ma2.Section, copy: Boolean = true ): PartitionedFragment = {
    val newSection = roiSection.intersect( cutSection ).shiftOrigin( roiSection )
    val indices = PartitionedFragment.sectionToIndices(newSection)
    val newDataArray: Nd4jMaskedTensor = array( indices )
    new PartitionedFragment( if(copy) newDataArray.dup else newDataArray, newSection )
  }

  def cutNewSubset( newSection: ma2.Section, copy: Boolean = true ): PartitionedFragment = {
    if (roiSection.equals( newSection )) this
    else {
      val relativeSection = newSection.shiftOrigin( roiSection )
      val newDataArray: Nd4jMaskedTensor = array( PartitionedFragment.sectionToIndices(relativeSection) )
      new PartitionedFragment( if(copy) newDataArray.dup else newDataArray, newSection )
    }
  }
  def size: Long = roiSection.computeSize
  def contains( requestedSection: ma2.Section ): Boolean = roiSection.contains( requestedSection )
}

object sectionTest extends App {
  val s0 = new ma2.Section( Array(10,10,0), Array(100,100,10) )
  val s1 = new ma2.Section( Array(50,50,5), Array(10,10,1) )
  val s2 = s1.shiftOrigin( s0 )
  println( s2 )
}

