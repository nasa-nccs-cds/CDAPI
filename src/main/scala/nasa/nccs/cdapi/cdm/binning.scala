package nasa.nccs.cdapi.cdm
import java.util.Formatter
import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.runtime._
import scala.reflect.runtime.universe._


trait BinAccumulator {
  def reset: Unit
  def insert( value: Float ): Unit
  def nresults: Int
  def result( index: Int = 0 ): Option[Float]
}

trait BinSliceAccumulator {
  def reset: Unit
  def insert( values: Nd4jMaskedTensor ): Unit
  def nresults: Int
  def result( index: Int = 0 ): Option[Nd4jMaskedTensor]
}

object BinSpec {
  def apply( binSpec: String ) = {
    val binSpecs = binSpec.split('|')
    val axis = binSpecs(0).toLowerCase.trim.head
    val step = binSpecs(1).toLowerCase.trim
    val cycle = if (binSpecs.length > 2) binSpecs(2).toLowerCase.trim else ""
    new BinSpec( axis, step, cycle )
  }
}

class BinSpec( val axis: Char, val step: String, val cycle: String )

class BinnedArrayBase[T: TypeTag]( private val nbins: Int ) {
  private val ttag = typeTag[T]
  private val ctor = currentMirror.reflectClass(ttag.tpe.typeSymbol.asClass).reflectConstructor(ttag.tpe.members.filter(m => m.isMethod && m.asMethod.isConstructor).iterator.toSeq(0).asMethod)
  protected val _accumulatorArray: IndexedSeq[T] = (0 until nbins).map(ival => ctor().asInstanceOf[T])
}

//class BinnedArray[ T<:BinAccumulator: TypeTag ](private val binIndices: Array[Int], nbins: Int ) extends BinnedArrayBase[T]( nbins ) {
//  def insert( binIndex: Int, value: Float ): Unit = _accumulatorArray( binIndices(binIndex) ).insert( value )
//  def result: Array[Float] = _accumulatorArray.map( _.result ).toArray
//}

class BinnedSliceArray[ T<:BinSliceAccumulator: TypeTag ](private val binIndices: Array[Int], private val nbins: Int )  extends BinnedArrayBase[T]( nbins )  {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  private var refSliceOpt: Option[Nd4jMaskedTensor]  = None
  def nresults = _accumulatorArray(0).nresults
  def result( result_index: Int = 0 ): Array[Nd4jMaskedTensor] = (0 until nbins).map( getAccumulatorResult(_,result_index) ).toArray

  def insert( binIndex: Int, values: Nd4jMaskedTensor ): Unit = {
    val bin_index = binIndices(binIndex)
    _accumulatorArray( bin_index ).insert( values )
//    logger.info( " Insert slice [%s] values = %s into bin %d, accum values = %s ".format(values.shape.mkString(","), values.toDataString,  bin_index, _accumulatorArray( bin_index ).toString ) )
    if(refSliceOpt == None) refSliceOpt = Some(values)
  }

  private def getAccumulatorResult( bin_index: Int, result_index: Int ): Nd4jMaskedTensor =
    _accumulatorArray(bin_index).result(result_index) match {
      case Some(result_array) => result_array;
      case None => refSliceOpt match { case Some(refSlice) => refSlice.invalids; case x => throw new Exception( "Attempt to obtain result from empty accumulator") }
    }
}

object BinnedSliceArray {
  import ucar.nc2.constants.AxisType
  import ucar.nc2.time.CalendarPeriod.Field._
  import ucar.nc2.time.CalendarDate
  import ucar.nc2.dataset.{CoordinateAxis1DTime, CoordinateAxis1D}

  def apply[ T<:BinSliceAccumulator: TypeTag ](dataset: CDSDataset, binSpec: BinSpec  ): BinnedSliceArray[T] = {
    val coord_axis: CoordinateAxis1D = dataset.getCoordinateAxis( binSpec.axis ) match {
      case caxis: CoordinateAxis1D => caxis;
      case x => throw new Exception("Coordinate Axis type %s can't currently be binned".format(x.getClass.getName))
    }
    val units = coord_axis.getUnitsString

    coord_axis.getAxisType match {
      case AxisType.Time =>
        val tcoord_axis = CoordinateAxis1DTime.factory(dataset.ncDataset, coord_axis, new Formatter())
        binSpec.step match {
          case "month" =>
            if (binSpec.cycle == "year") {
              val binIndices = tcoord_axis.getCalendarDates.map( _.getFieldValue(Month)-1 ).toArray
              new BinnedSliceArray[T]( binIndices, 12 )
            } else {
              val year_offset = tcoord_axis.getCalendarDate(0).getFieldValue(Year)
              val binIndices = tcoord_axis.getCalendarDates.map( cdate => cdate.getFieldValue(Month)-1 + cdate.getFieldValue(Year) - year_offset ).toArray
              new BinnedSliceArray[T]( binIndices, coord_axis.getShape(0) )
            }
        }
      case x => throw new Exception("Binning not yet implemented for this axis type: %s".format(x.getClass.getName))
    }
  }
}

class aveSliceAccumulator extends BinSliceAccumulator {
  private var _value: Option[Nd4jMaskedTensor] = None
  private var _count = 0
  def nresults = 1
  def reset: Unit = { _value = None; _count = 0 }
  override def toString = _value match { case None => "None"; case Some(mtensor) => "Accumulator{ count = %d, value = %s }".format( _count, mtensor.toDataString ) }

  private def accumulator( template: Nd4jMaskedTensor ): Nd4jMaskedTensor = {
    if( _value == None ) _value = Some( template.zeros )
    _value.get
  }

  def insert( values: Nd4jMaskedTensor ): Unit = {
    accumulator(values) += values
    _count += 1
  }

  def result( index: Int = 0 ): Option[Nd4jMaskedTensor] = index match {
    case 0 => _value match { case None => None;  case Some(accum_array) => Some(accum_array / _count) }
    case x => None
  }
}



//class aveAccumulator extends BinAccumulator {
//  var _value = 0f
//  var _count = 0
//  def reset: Unit = { _value = 0f; _count = 0 }
//  def insert( value: Float ): Unit = { _value+= value; _count += 1 }
//  def result = Array(_value/_count )
//  def nresults = 1
//}
//
//class sumAccumulator extends BinAccumulator {
//  var _value = 0f
//  def reset: Unit = { _value = 0f }
//  def insert( value: Float ): Unit = { _value += value }
//  def result: Array[Float] =  Array(_value)
//  def nresults = 1
//}
//
//class maxminAccumulator extends BinAccumulator {
//  var _max = Float.NegativeInfinity
//  var _min = Float.PositiveInfinity
//  def reset: Unit = { _max = Float.NegativeInfinity;  _min = Float.PositiveInfinity }
//  def insert( value: Float ): Unit = { _max = math.max(value,_max); _min = math.min(value,_min) }
//  def result: Array[Float] =  Array(_max,_min)
//  def nresults = 2
//}



