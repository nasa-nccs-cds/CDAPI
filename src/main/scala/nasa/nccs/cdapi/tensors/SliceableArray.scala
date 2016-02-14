package nasa.nccs.cdapi.tensors

/**
 * A sliceable array.
 */
trait SliceableArray {

  type T <: SliceableArray

  def rows: Int

  def cols: Int

  def shape: Array[Int]

  def data: Array[Float]

  def apply(ranges: (Int, Int)*): T

  def apply(indexes: Int*): Float

}