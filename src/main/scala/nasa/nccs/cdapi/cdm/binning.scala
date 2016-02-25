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
  def result: Array[Float]
}

trait BinSliceAccumulator {
  def reset: Unit
  def insert( values: Nd4jMaskedTensor ): Unit
  def result: Array[Nd4jMaskedTensor]
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
  private val T_tt = typeTag[T]
  private val T_ctor = currentMirror.reflectClass(T_tt.tpe.typeSymbol.asClass).reflectConstructor(T_tt.tpe.members.filter(m => m.isMethod && m.asMethod.isConstructor).iterator.toSeq(0).asMethod)
  protected val _accumulatorArray: IndexedSeq[T] = (0 until nbins).map(ival => T_ctor().asInstanceOf[T])
}

class BinnedArray[ T<:BinAccumulator: TypeTag ](private val binIndices: Array[Int], nbins: Int ) extends BinnedArrayBase[T]( nbins ) {
  def insert( binIndex: Int, value: Float ): Unit = _accumulatorArray( binIndices(binIndex) ).insert( value )
  def result: Array[Array[Float]] = _accumulatorArray.map( _.result ).toArray
}

class BinnedSliceArray[ T<:BinSliceAccumulator: TypeTag ](private val binIndices: Array[Int], private val nbins: Int )  extends BinnedArrayBase[T]( nbins )  {
  def insert( binIndex: Int, values: Nd4jMaskedTensor ): Unit = _accumulatorArray( binIndices(binIndex) ).insert( values )
  def result: Array[Array[Nd4jMaskedTensor]] = _accumulatorArray.map( _.result ).toArray
}

object BinnedArray {
  import ucar.nc2.constants.AxisType
  import ucar.nc2.time.CalendarPeriod.Field._
  import ucar.nc2.time.CalendarDate
  import ucar.nc2.dataset.{CoordinateAxis1DTime, CoordinateAxis1D}

  def apply[ T<:BinAccumulator: TypeTag ](dataset: CDSDataset, binSpec: BinSpec  ) = {
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
              val binIndices = tcoord_axis.getCalendarDates.map( _.getFieldValue(Month) ).toArray
              new BinnedArray[T]( binIndices, 12 )
            } else {
              val year_offset = tcoord_axis.getCalendarDate(0).getFieldValue(Year)
              val binIndices = tcoord_axis.getCalendarDates.map( cdate => cdate.getFieldValue(Month) + cdate.getFieldValue(Year) - year_offset ).toArray
              new BinnedArray[T]( binIndices, coord_axis.getShape(0) )
            }
        }
      case x => throw new Exception("Binning not yet implemented for this axis type: %s".format(x.getClass.getName))
    }
  }
}

class aveSliceAccumulator( invalid: Float, val shape: Int* ) extends BinSliceAccumulator {
  var _value: Nd4jMaskedTensor = new Nd4jMaskedTensor( Nd4j.zeros(shape:_*), invalid )
  var _count = 0
  def reset: Unit = { _value.tensor.assign(0f); _count = 0 }
  def insert( values: Nd4jMaskedTensor ): Unit = { _value += values; _count += 1 }
  def result: Nd4jMaskedTensor = _value/_count
}

class aveAccumulator extends BinAccumulator {
  var _value = 0f
  var _count = 0
  def reset: Unit = { _value = 0f; _count = 0 }
  def insert( value: Float ): Unit = { _value+= value; _count += 1 }
  def result = Array(_value/_count )
}

class sumAccumulator extends BinAccumulator {
  var _value = 0f
  def reset: Unit = { _value = 0f }
  def insert( value: Float ): Unit = { _value += value }
  def result: Array[Float] =  Array(_value)
}

class maxminAccumulator extends BinAccumulator {
  var _max = Float.NegativeInfinity
  var _min = Float.PositiveInfinity
  def reset: Unit = { _max = Float.NegativeInfinity;  _min = Float.PositiveInfinity }
  def insert( value: Float ): Unit = { _max = math.max(value,_max); _min = math.min(value,_min) }
  def result: Array[Float] =  Array(_max,_min)
}



