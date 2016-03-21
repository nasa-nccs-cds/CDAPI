package nasa.nccs.cdapi.cdm
import java.util.Formatter
import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor
import nasa.nccs.utilities.cdsutils
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis1DTime, CoordinateAxis1D}
import ucar.nc2.time.CalendarPeriod.Field._

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

object BinnedArrayFactory {
  def apply( binSpec: String, variable: CDSVariable ) = {
    val binSpecs = binSpec.split('|')
    val axis = binSpecs(0).toLowerCase.trim.head
    val step = binSpecs(1).toLowerCase.trim
    val reducer = if (binSpecs.length > 2) binSpecs(2).toLowerCase.trim else "ave"
    val cycle = if (binSpecs.length > 3) binSpecs(3).toLowerCase.trim else ""
    new BinnedArrayFactory( axis, step, reducer, cycle, variable )
  }
}

class BinnedArrayFactory( val axis: Char, val step: String, val reducer: String, val cycle: String, variable: CDSVariable ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  private case class SliceArraySpec( nBins: Int, binIndices: Array[Int]  )
  lazy val dimension: Int = variable.getAxisIndex( axis )
  lazy val coordinateAxis: CoordinateAxis1D = variable.dataset.getCoordinateAxis( axis ) match {
    case caxis: CoordinateAxis1D => caxis;
    case x => throw new Exception("Coordinate Axis type %s can't currently be binned".format(x.getClass.getName))
  }
  lazy val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(variable.dataset.ncDataset, coordinateAxis, new Formatter())

  private def createArrayInstance( sliceArraySpec: SliceArraySpec ): IBinnedSliceArray = {
    reducer match {
      case "ave" =>
        val binIndices = sliceArraySpec.binIndices
        val nBins = sliceArraySpec.nBins
        cdsutils.time( logger, "new BinnedSliceArray" ) { new BinnedAveSliceArray( binIndices, nBins, dimension ) }
      case x => throw new Exception("Binning not yet implemented for this reducer type: %s".format(reducer))
    }
  }

  def getBinnedArray: IBinnedSliceArray = {
    val units = coordinateAxis.getUnitsString
    val sliceArraySpec = coordinateAxis.getAxisType match {
      case AxisType.Time =>
        step match {
          case "month" =>
            if (cycle == "year") {
              val binIndices: Array[Int] = cdsutils.time( logger, "binIndices" )( timeAxis.getCalendarDates.map( _.getFieldValue(Month)-1 ).toArray )
              cdsutils.time( logger, "SliceArraySpec" )( SliceArraySpec( 12, binIndices ) )
            } else {
              val year_offset = timeAxis.getCalendarDate(0).getFieldValue(Year)
              new SliceArraySpec( coordinateAxis.getShape(0), timeAxis.getCalendarDates.map( cdate => cdate.getFieldValue(Month)-1 + cdate.getFieldValue(Year) - year_offset ).toArray )
            }
          case x => throw new Exception("Binning not yet implemented for this step type: %s".format(step))
        }
      case x => throw new Exception("Binning not yet implemented for this axis type: %s".format(x.getClass.getName))
    }
    createArrayInstance( sliceArraySpec )
  }
}

trait IBinnedSliceArray {
  def nresults: Int
  def insert( binIndex: Int, values: Nd4jMaskedTensor ): Unit
  def result( result_index: Int = 0 ): Option[Nd4jMaskedTensor]
}

class BinnedArrayBase[T: TypeTag]( private val nbins: Int ) {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  protected val _accumulatorArray: IndexedSeq[T] = getAccumArray
    private def getAccumArray: IndexedSeq[T] = cdsutils.time(logger, "BinnedArray:getAccumArray") {
      val ttag = typeTag[T]
    lazy val ctor = currentMirror.reflectClass(ttag.tpe.typeSymbol.asClass).reflectConstructor(ttag.tpe.members.filter(m => m.isMethod && m.asMethod.isConstructor).iterator.toSeq.head.asMethod)
    (0 until nbins).map(ival => ctor().asInstanceOf[T])
  }
}

//class BinnedArray[ T<:BinAccumulator: TypeTag ](private val binIndices: Array[Int], nbins: Int ) extends BinnedArrayBase[T]( nbins ) {
//  def insert( binIndex: Int, value: Float ): Unit = _accumulatorArray( binIndices(binIndex) ).insert( value )
//  def result: Array[Float] = _accumulatorArray.map( _.result ).toArray
//}

class BinnedSliceArray[ T<:BinSliceAccumulator: TypeTag ](private val binIndices: Array[Int], private val nbins: Int )  extends BinnedArrayBase[T]( nbins ) with IBinnedSliceArray  {
  private var refSliceOpt: Option[Nd4jMaskedTensor]  = None
  def nresults = _accumulatorArray(0).nresults
  def result( result_index: Int = 0 ): Option[Nd4jMaskedTensor] = {
    val result_masked_arrays = (0 until nbins).map( getAccumulatorResult(_,result_index) ).toArray
    Some( new Nd4jMaskedTensor( Nd4j.concat( 0, result_masked_arrays.map(_.tensor): _* ), result_masked_arrays(0).invalid ) )
  }

  def insert( binIndex: Int, values: Nd4jMaskedTensor ): Unit = {
    val bin_index = binIndices(binIndex)
    _accumulatorArray( bin_index ).insert( values )
//    logger.info( " Insert slice [%s] values = %s into bin %d, accum values = %s ".format(values.shape.mkString(","), values.toDataString,  bin_index, _accumulatorArray( bin_index ).toString ) )
    if(refSliceOpt.isEmpty) refSliceOpt = Some(values)
  }

  private def getAccumulatorResult( bin_index: Int, result_index: Int ): Nd4jMaskedTensor =
    _accumulatorArray(bin_index).result(result_index) match {
      case Some(result_array) => result_array;
      case None => refSliceOpt match { case Some(refSlice) => refSlice.invalids; case x => throw new Exception( "Attempt to obtain result from empty accumulator") }
    }
}

class aveSliceAccumulator extends BinSliceAccumulator {
  private var _value: Option[Nd4jMaskedTensor] = None
  private var _count = 0
  def nresults = 1
  def reset: Unit = { _value = None; _count = 0 }
  override def toString = _value match { case None => "None"; case Some(mtensor) => "Accumulator{ count = %d, value = %s }".format( _count, mtensor.toDataString ) }

  private def accumulator( template: Nd4jMaskedTensor ): Nd4jMaskedTensor = {
    if( _value.isEmpty ) _value = Some( template.zeros )
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

class BinnedAveSliceArray( private val binIndices: Array[Int], private val nbins: Int, val dimension: Int )  extends IBinnedSliceArray  {
  var _values:  Option[Nd4jMaskedTensor] = None
  var _counts: Option[Nd4jMaskedTensor] = None
  def nresults = 1
  private def getBinsArray( template: Nd4jMaskedTensor  ): Nd4jMaskedTensor = new Nd4jMaskedTensor( Nd4j.zeros( template.shape.updated(dimension,nbins): _* ), template.invalid )
  private def initValues( template: Nd4jMaskedTensor ):  Unit = { if( _values.isEmpty ) _values = Some( getBinsArray( template ) ) }
  private def initCounter( template: Nd4jMaskedTensor ): Unit = { if( _counts.isEmpty ) _counts = Some( getBinsArray( template ) ) }
  private def accumulator( template: Nd4jMaskedTensor ): Nd4jMaskedTensor = { initValues(template); _values.get }
  private def binCounts( template: Nd4jMaskedTensor ):   Nd4jMaskedTensor =  { initCounter(template); _counts.get }

  def insert( binIndex: Int, values: Nd4jMaskedTensor ): Unit =
    accumulator(values).slice( binIndices(binIndex), dimension ) :++= ( values, binCounts(values).slice( binIndices(binIndex), dimension ) )

  def result( result_index: Int = 0 ): Option[Nd4jMaskedTensor] = result_index match {
    case 0 => _values match {
      case None => None;
      case Some( values ) => Some(values :/ _counts.get) };
    case x => None
  }
}

//object binTest extends App {
//
//  val array = new Nd4jMaskedTensor( Nd4j.create( Array.fill[Float](100)(1f), Array(25,2,2)) )
//  (0 until 25 by 5).foreach( iC => array.tensor.putScalar( Array(iC,0,0), Float.MaxValue ) )
//  array.tensor.putScalar( Array(0,1,1), Float.MaxValue )
//  val bin_array = (0 until 25).map( _ % 5 ).toArray
//  val binAccumulator = new BinnedAveSliceArray( bin_array, 5 )
//  ( 0 until array.shape(0) ).foreach( index => binAccumulator.insert( index, array.slice(index) ))
//  val result = binAccumulator.result(0)
//  println( result(0).tensor.toString )
//  println( binAccumulator._values.get.tensor.toString )
//  println( binAccumulator._counts.get.tensor.toString )
//}

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



