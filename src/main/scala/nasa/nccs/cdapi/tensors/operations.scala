package nasa.nccs.cdapi.tensors

import ucar.ma2

abstract class TensorAccumulatorOp1( val invalid: Float = Float.MaxValue ) {
  def init: Unit
  def insert( values: Float* ): Unit
  def result: Float
}

abstract class TensorAccumulatorOpN( val invalid: Float = Float.MaxValue ) {
  def init: Unit
  def insert( values: Float* ): Unit
  def result: Array[Float]
  def length: Int
}

abstract class TensorCombinerOp( val invalid: Float = Float.MaxValue ) {
  def init: Unit = {}
  def combine( value0: Float, value1: Float ): Float
  def accumulate( value0: Float, value1: Float ): Float
}

class meanOp(invalid: Float) extends TensorAccumulatorOp1(invalid) {
  var value_sum  = 0f
  var value_count = 0f
  def init = { value_sum  = 0f; value_count = 0f }
  def insert( values: Float* ) = if(values(0) != invalid) if ( values.length == 1 ) { value_sum += values(0); value_count += 1 } else { value_sum += values(0)*values(1); value_count += values(1) }
  def result = value_sum / value_count
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
  def accumulate( value0: Float, value1: Float ): Float = {
    if(value0 == invalid) return value1
    if(value1 == invalid) return value0
    value0 * value1
  }
}
object multOp { def apply(invalid: Float) = { new multOp(invalid) } }


