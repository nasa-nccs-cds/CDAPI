package nasa.nccs.cdapi.tensors

trait TensorOp {
  def init: Unit
  def insert( value: Float ): Unit
  def result: Array[Float]
  def length: Int

}

object meanOp extends TensorOp {
  var value_sum  = 0f
  var value_count = 0
  def init = { value_sum  = 0f; value_count = 0 }
  def insert( value: Float ) = { value_sum += value; value_count += 1 }
  def result = Array( value_sum / value_count )
  def length = 1
}

