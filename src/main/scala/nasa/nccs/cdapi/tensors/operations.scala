package nasa.nccs.cdapi.tensors

abstract class TensorAccumulatorOp( val invalid: Float = Float.NaN ) {
  def init: Unit
  def insert( value: Float ): Unit
  def result: Array[Float]
  def length: Int
}

abstract class TensorCombinerOp( val invalid: Float = Float.NaN ) {
  def init: Unit = {}
  def combine( value0: Float, value1: Float ): Float
  def accumulate( value0: Float, value1: Float ): Float
}

class meanOp(invalid: Float) extends TensorAccumulatorOp(invalid) {
  var value_sum  = 0f
  var value_count = 0
  def init = { value_sum  = 0f; value_count = 0 }
  def insert( value: Float ) = if(value != invalid) { value_sum += value; value_count += 1 }
  def result = Array( value_sum / value_count )
  def length = 1
}

class subOp(invalid: Float) extends TensorCombinerOp(invalid) {
  def combine( value0: Float, value1: Float ): Float = {
    if( (value0 == invalid) || (value1 == invalid) ) return invalid
    value0 - value1
  }
  def accumulate( value0: Float, value1: Float ): Float = {
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
  def accumulate( value0: Float, value1: Float ): Float = {
    if(value0 == invalid) return value1
    if(value1 == invalid) return value0
    value0 + value1
  }
}
object addOp { def apply(invalid: Float) = { new addOp(invalid) } }

class divOp(invalid: Float) extends TensorCombinerOp(invalid) {
  def combine( value0: Float, value1: Float ): Float = {
    if( (value0 == invalid) || (value1 == invalid) ) return invalid
    value0 / value1
  }

  def accumulate( value0: Float, value1: Float ): Float = {
    if( (value0 == invalid) || (value1 == invalid) ) return invalid
    value0 / value1
  }
}
object divOp { def apply(invalid: Float) = { new divOp(invalid) } }

class multOp(invalid: Float) extends TensorCombinerOp(invalid) {
  def combine( value0: Float, value1: Float ): Float = {
    if( (value0 == invalid) || (value1 == invalid) ) return invalid
    value0 * value1
  }
  def accumulate( value0: Float, value1: Float ): Float = {
    if(value0 == invalid) return value1
    if(value1 == invalid) return value0
    value0 * value1
  }
}
object multOp { def apply(invalid: Float) = { new multOp(invalid) } }


