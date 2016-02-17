package nasa.nccs.cdapi.tensors

import org.slf4j.LoggerFactory

trait OpAccumulator {
  def insert( value: Float ): Unit
}

trait TensorOp extends OpAccumulator {
  def init(): Unit
  def result(): Array[Float]
  def length: Int
}

trait AbstractTensor  extends Serializable with SliceableArray {

  type T <: AbstractTensor
  val name: String
  val logger = LoggerFactory.getLogger(this.getClass)

  def zeros(shape: Int*): T

  def exec( op: TensorOp, dimensions: Int* ): T

  def put(value: Float, shape: Int*): Unit

  def +(array: AbstractTensor): T

  def -(array: AbstractTensor): T

  def *(array: AbstractTensor): T

  def /(array: AbstractTensor): T

  def \(array: AbstractTensor): T

  def **(array: AbstractTensor): T
  
  def div(num: Float): T

  def <=(num: Float): T

  def :=(num: Float): T

  def cumsum: Float

  def mean( dimensions: Int* ): T

  def toString: String

  def shape: Array[Int]

  def isZero: Boolean

  def max: Float

  def min: Float

  def dup(): T

}
