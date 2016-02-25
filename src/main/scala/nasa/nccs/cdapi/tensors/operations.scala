package nasa.nccs.cdapi.tensors

trait TensorAccumulatorOp {
  def init: Unit
  def insert( value: Float ): Unit
  def result: Array[Float]
  def length: Int
}

trait TensorCombinerOp {
  def init: Unit = {}
  def combine( value0: Float, value1: Float ): Float

}

object meanOp extends TensorAccumulatorOp {
  var value_sum  = 0f
  var value_count = 0
  def init = { value_sum  = 0f; value_count = 0 }
  def insert( value: Float ) = { value_sum += value; value_count += 1 }
  def result = Array( value_sum / value_count )
  def length = 1
}

object subOp extends TensorCombinerOp {
  def combine( value0: Float, value1: Float ): Float = { value0 - value1 }
}
object addOp extends TensorCombinerOp {
  def combine( value0: Float, value1: Float ): Float = { value0 + value1 }
}

object divOp extends TensorCombinerOp {
  def combine( value0: Float, value1: Float ): Float = { value0 / value1 }
}

object multOp extends TensorCombinerOp {
  def combine( value0: Float, value1: Float ): Float = { value0 * value1 }
}



