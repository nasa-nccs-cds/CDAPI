package nasa.nccs.cdapi.tensors

abstract class TensorAccumulatorOp( val invalid: Float = Float.MaxValue ) {
  def insert( values: Float* ): Unit
  def getResult: Array[Float]
}

abstract class TensorCombinerOp( val invalid: Float = Float.MaxValue ) {
  def combine( value0: Float, value1: Float ): Float
}

class meanOp(invalid: Float) extends TensorAccumulatorOp(invalid) {
  var value_sum  = 0f
  var value_count = 0f
  def insert( values: Float* ) = if(values(0) != invalid) if ( values.length == 1 ) { value_sum += values(0); value_count += 1 } else { value_sum += values(0)*values(1); value_count += values(1) }
  def getResult = Array( value_sum / value_count )
}

class subOp(invalid: Float) extends TensorCombinerOp(invalid) {
  def combine( value0: Float, value1: Float ): Float = {
    if( (value0 == invalid) || (value1 == invalid) ) return invalid
    value0 - value1
  }
}
object subOp { def apply(invalid: Float) = { new subOp(invalid) } }

class addOp(invalid: Float) extends TensorCombinerOp(invalid) {
  def combine( value0: Float, value1: Float ): Float = {
    if( (value0 == invalid) || (value1 == invalid) ) return invalid
    value0 + value1
  }
}
object addOp { def apply(invalid: Float) = { new addOp(invalid) } }

class divOp(invalid: Float) extends TensorCombinerOp(invalid) {
  def combine( value0: Float, value1: Float ): Float = {
    if( (value0 == invalid) || (value1 == invalid) || (value1 == 0.0) ) return invalid
    value0 / value1
  }
}
object divOp { def apply(invalid: Float) = { new divOp(invalid) } }

class multOp(invalid: Float) extends TensorCombinerOp(invalid) {
  def combine( value0: Float, value1: Float ): Float = {
    if( (value0 == invalid) || (value1 == invalid) ) return invalid
    value0 * value1
  }
}
object multOp { def apply(invalid: Float) = { new multOp(invalid) } }


