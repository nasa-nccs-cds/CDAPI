package nasa.nccs.cdapi.tensors

import nasa.nccs.cdapi.cdm._
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.{NDArrayIndex, INDArrayIndex}
import scala.reflect.runtime._
import scala.reflect.runtime.universe._


// import org.nd4s.Implicits._
import scala.collection.immutable.IndexedSeq
// import scala.language.implicitConversions
//import scala.collection.JavaConversions._
////import scala.collection.JavaConverters._

object Nd4jM {
  def concat( dimension: Int, mTensors: Array[Nd4jMaskedTensor] ): Nd4jMaskedTensor = {
    assert( mTensors.length > 0, "Attempt to concatenate empty collection of arrays")
    val tensor = Nd4j.concat( dimension, mTensors.map(_.tensor):_*  )
    new Nd4jMaskedTensor( tensor, mTensors(0).invalid )
  }
}

class Nd4jMaskedTensor( val tensor: INDArray = Nd4j.create(0), val invalid: Float = Float.NaN ) extends Serializable {
  val name: String = "Nd4jMaskedTensor"
  val shape = tensor.shape

  override def toString =  "%s[%s]".format( name, shape.mkString(",") )
  def rows = tensor.rows
  def cols = tensor.columns
  def data: Array[Float] = tensor.data.asFloat
  def toRawDataString = data.mkString("[ ",", "," ]")
  def toDataString = tensor.toString
 // def apply( indices: Int*): Float = tensor.getFloat( indices )

  def masked( tensor: INDArray ) = new Nd4jMaskedTensor( tensor, invalid )

  def slice( slice_index: Int, dimension: Int ): Nd4jMaskedTensor = {
    val offsets = Array.fill[Int](shape.length)(0).updated(dimension,slice_index)
    val stride = Array.fill[Int](shape.length)(1)
    val newshape = shape.updated(dimension,1)
    new Nd4jMaskedTensor( tensor.subArray( offsets, newshape, stride ), invalid )
  }

  def execAccumulatorOp(op: TensorAccumulatorOp, dimensions: Int*): Nd4jMaskedTensor = {
    val filtered_shape: IndexedSeq[Int] = (0 until shape.length).flatMap(x => if (dimensions.exists(_ == x)) None else Some(shape(x)))
    val slices = Nd4j.concat(0, (0 until filtered_shape.product).map(iS => Nd4j.create(subset(iS, dimensions: _*).applyAccumulatorOp(op))): _*)
    slices.reshape(filtered_shape :+ op.length: _* )
    new Nd4jMaskedTensor( slices, invalid )
  }
  def execCombinerOp(op: TensorCombinerOp, maskedArray: Nd4jMaskedTensor): Nd4jMaskedTensor = {
    val result_array = Nd4j.create( applyCombinerOp( op, maskedArray ), shape )
    new Nd4jMaskedTensor( result_array, invalid )
  }
  def execCombinerOp(op: TensorCombinerOp, value: Float): Nd4jMaskedTensor = {
    val result_array = Nd4j.create( applyCombinerOp( op, value ), shape )
    new Nd4jMaskedTensor( result_array, invalid )
  }

  def execBinningOp( dimension: Int, binFactory: BinnedArrayFactory ): List[Nd4jMaskedTensor] = {
    val bins: IBinnedSliceArray = binFactory.getBinnedArray
    (0 until tensor.shape()(dimension)).map( iS => bins.insert( iS, slice( iS, dimension ) ) )
    (0 until bins.nresults).map( iR => Nd4jM.concat( dimension, bins.result(iR) ) ).toList
  }

  def dup: Nd4jMaskedTensor = { new Nd4jMaskedTensor( tensor.dup, invalid ) }

  def broadcast( shape: Int* ) = new Nd4jMaskedTensor( tensor.broadcast(shape:_*), invalid )

  def subset( index: Int, dimensions: Int*  ): Nd4jMaskedTensor = {
    new Nd4jMaskedTensor( tensor.tensorAlongDimension(index, dimensions:_*), invalid )
  }

  def applyAccumulatorOp( op: TensorAccumulatorOp ): Array[Float] = {
    op.init
    ( 0 until tensor.length ).map( iC =>  {
      val v = tensor.getFloat(iC)
      if( v != invalid ) op.insert(v)
    } )
    op.result
  }

  def applyCombinerOp( op: TensorCombinerOp, maskedArray: Nd4jMaskedTensor ): Array[Float] = {
    op.init
    val result = ( 0 until tensor.length ).map( iC => {
      val v0 = tensor.getFloat(iC)
      val v1 = maskedArray.tensor.getFloat(iC)
      if( (v0 == invalid) || (v1 == maskedArray.invalid) ) invalid else op.combine(v0,v1)
    } )
    result.toArray
  }

  def applyCombinerOp( op: TensorCombinerOp, value: Float ): Array[Float] = {
    op.init
    val result = ( 0 until tensor.length ).map( iC => {
      val v0 = tensor.getFloat(iC)
      if( v0 == invalid ) invalid else op.combine(v0,value)
    } )
    result.toArray
  }

  def applyInPlaceCombinerOp( op: TensorCombinerOp, maskedArray: Nd4jMaskedTensor ): Unit = {
    op.init
    ( 0 until tensor.length ).map( iC => {
      val v0 = tensor.getFloat(iC)
      val v1 = maskedArray.tensor.getFloat(iC)
      val result = if( (v0 == invalid) || (v1 == maskedArray.invalid) ) invalid else op.combine(v0,v1)
//      println( "applyInPlaceCombinerOp[%d]: %f & %f -> %f".format( iC, v0, v1, result) )
      tensor.putScalar( iC, result )
    } )
  }

  def applyInPlaceCombinerOp( op: TensorCombinerOp, value: Float ): Unit = {
    op.init
    ( 0 until tensor.length ).map( iC =>  {
      val v0 = tensor.getFloat(iC)
      val result = if( v0 == invalid ) invalid else op.combine(v0,value)
      tensor.putScalar( iC, result )
    } )
  }

  def mean( dimensions: Int* ): Nd4jMaskedTensor = execAccumulatorOp( meanOp, dimensions:_* )

  def bin( dimension: Int, binFactory: BinnedArrayFactory ): List[Nd4jMaskedTensor] = execBinningOp( dimension, binFactory )

  def -(array: Nd4jMaskedTensor) = execCombinerOp( subOp, array )

  def +(array: Nd4jMaskedTensor) = execCombinerOp( addOp, array )

  def /(array: Nd4jMaskedTensor) = execCombinerOp( divOp, array )

  def *(array: Nd4jMaskedTensor) = execCombinerOp( multOp, array )

  def -(value: Float) = execCombinerOp( subOp, value )

  def +(value: Float) = execCombinerOp( addOp, value )

  def /(value: Float) = execCombinerOp( divOp, value )

  def *(value: Float) = execCombinerOp( multOp, value )

  def -=(array: Nd4jMaskedTensor) = applyInPlaceCombinerOp( subOp, array )

  def +=(array: Nd4jMaskedTensor) = applyInPlaceCombinerOp( addOp, array )

  def /=(array: Nd4jMaskedTensor) = applyInPlaceCombinerOp( divOp, array )

  def *=(array: Nd4jMaskedTensor) = applyInPlaceCombinerOp( multOp, array )

  def -=(value: Float) = applyInPlaceCombinerOp( subOp, value )

  def +=(value: Float) = applyInPlaceCombinerOp( addOp, value )

  def /=(value: Float) = applyInPlaceCombinerOp( divOp, value )

  def *=(value: Float) = applyInPlaceCombinerOp( multOp, value )

  def rawmean( dimensions: Int* ): Nd4jMaskedTensor =  new Nd4jMaskedTensor( tensor.mean(dimensions:_*), invalid )

  def apply( ranges: List[INDArrayIndex] ) = {
    val IndArray = tensor.get(ranges: _*)
    new Nd4jMaskedTensor(IndArray)
  }

  def zeros: Nd4jMaskedTensor = new Nd4jMaskedTensor( Nd4j.zerosLike(tensor), invalid )
  def invalids: Nd4jMaskedTensor = new Nd4jMaskedTensor( Nd4j.emptyLike(tensor).assign(invalid), invalid )

  /*

  def zeros(shape: Int*) = new Nd4jMaskedTensor(Nd4j.create(shape: _*))

  def map(f: Double => Double) = new Nd4jMaskedTensor(tensor.map(p => f(p)))

  def put(value: Float, shape: Int*) = tensor.putScalar(shape.toArray, value)

  def +(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.add(array.tensor))

  def -(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.sub(array.tensor))

  def \(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.div(array.tensor))

  def /(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.div(array.tensor))

  def *(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.mul(array.tensor))

  /**
   * Masking operations
   */

  def <=(num: Float): Nd4jMaskedTensor = new Nd4jMaskedTensor(tensor.map(p => if (p < num) p else 0.0))

  def :=(num: Float) = new Nd4jMaskedTensor(tensor.map(p => if (p == num) p else 0.0))

  /**
   * Linear Algebra Operations
   */
  
  def **(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.dot(array.tensor))
  
  def div(num: Float): Nd4jMaskedTensor = new Nd4jMaskedTensor(tensor.div(num))

  /**
   * SliceableArray operations
   */

  def rows = tensor.rows
  
  def cols = tensor.columns

  def apply = this

  def apply(ranges: (Int, Int)*) = {
    val rangeMap = ranges.map(p => TupleRange(p))
    val indArray = tensor(rangeMap: _*)
    new Nd4jMaskedTensor(indArray,invalid)
  }

  def apply(indexes: Int*) = tensor.get(indexes.toArray).toFloat

  def data: Array[Float] = tensor.data.asFloat

  /**
   * Utility Functions
   */
  
  def cumsum = tensor.sumNumber.asInstanceOf[Float]

  def dup = new Nd4jMaskedTensor(tensor.dup)

  def isZero = tensor.mul(tensor).sumNumber.asInstanceOf[Float] <= 1E-9
  
  def isZeroShortcut = tensor.sumNumber().asInstanceOf[Float] <= 1E-9

  def max = tensor.maxNumber.asInstanceOf[Float]

  def min = tensor.minNumber.asInstanceOf[Float]

  private implicit def AbstractConvert(array: Nd4jMaskedTensor): Nd4jMaskedTensor = array.asInstanceOf[Nd4jMaskedTensor]
 */
}


