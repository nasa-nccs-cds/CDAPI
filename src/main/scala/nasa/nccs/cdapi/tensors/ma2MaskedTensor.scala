package nasa.nccs.cdapi.tensors
import ucar.ma2

object Ma2MaskedFloatTensor {

  def apply( tensor: ma2.Array, invalidOpt: Option[Float] ): Ma2MaskedFloatTensor = ma2.DataType.getType(tensor.getElementType) match {
    case ma2.DataType.FLOAT => new Ma2MaskedFloatTensor(tensor.asInstanceOf[ma2.ArrayFloat], invalidOpt)
    case ma2.DataType.DOUBLE => new Ma2MaskedFloatTensor( tensor.getStorage.asInstanceOf[Array[Double]].map(_.toFloat), tensor.getShape, invalidOpt )
    case ma2.DataType.INT =>    new Ma2MaskedFloatTensor( tensor.getStorage.asInstanceOf[Array[Int]].map(_.toFloat), tensor.getShape, invalidOpt )
  }
}


class Ma2MaskedFloatTensor( protected val tensor: ma2.ArrayFloat, val invalidOpt: Option[Float] = None  ) {

  def this( array: Array[Float], shape: Array[Int], invalidOpt: Option[Float] ) =
    this( ma2.Array.factory( ma2.DataType.FLOAT, shape, array).asInstanceOf[ma2.ArrayFloat], invalidOpt  )

  def this( npts: Int, start: Float, incr: Float, invalidOpt: Option[Float] ) =
    this( ma2.Array.makeArray(ma2.DataType.FLOAT, npts, start, incr).asInstanceOf[ma2.ArrayFloat], invalidOpt )

  def getStore: ma2.Array = tensor
  def getStoreShape: Array[Int] = tensor.getShape
  def getRank: Int = tensor.getRank
  def getElementType: ma2.DataType = ma2.DataType.getType(tensor.getElementType)
  def reshape( shape: Array[Int]): Ma2MaskedFloatTensor= wrap(tensor.reshape(shape))
  def wrap( new_tensor: ma2.Array ): Ma2MaskedFloatTensor = Ma2MaskedFloatTensor( new_tensor, invalidOpt )

  def section(origin: Array[Int], shape: Array[Int]) : Ma2MaskedFloatTensor= wrap(tensor.section(origin,shape))
  def section(origin: Array[Int], shape: Array[Int], stride: Array[Int]): Ma2MaskedFloatTensor= wrap(tensor.section(origin,shape,stride))
  def section(ranges: java.util.List[ma2.Range] ): Ma2MaskedFloatTensor= wrap(tensor.section(ranges))
  def sectionNoReduce(origin: Array[Int], shape: Array[Int], stride: Array[Int]): Ma2MaskedFloatTensor= wrap(tensor.sectionNoReduce(origin,shape,stride))
  def sectionNoReduce(ranges: java.util.List[ma2.Range] ): Ma2MaskedFloatTensor= wrap(tensor.sectionNoReduce(ranges))

  def slice(origin: Array[Int], shape: Array[Int]): Ma2MaskedFloatTensor= wrap(tensor.section(origin,shape))

  def	slice( dim: Int, value: Int ): Ma2MaskedFloatTensor= wrap(tensor.slice( dim, value ))
  def	transpose( dim1: Int, dim2: Int ): Ma2MaskedFloatTensor= wrap(tensor.transpose( dim1, dim2 ))
  def	permute( dims: Array[Int] ): Ma2MaskedFloatTensor= wrap(tensor.permute( dims ))
  def	reduce( dim: Int ): Ma2MaskedFloatTensor= wrap(tensor.reduce( dim ))
  def	flip( dim: Int ): Ma2MaskedFloatTensor= wrap(tensor.flip( dim ))
  def	reduce(  ): Ma2MaskedFloatTensor= wrap(tensor.reduce())
  def getFloat( index: Int ): Float = tensor.getFloat( index )
  def getFloat( index: ma2.Index ): Float = tensor.getFloat( index )

  //  def getDataArray: Array = ma2.DataType.getType(tensor.getElementType) match {
//    case ma2.DataType.FLOAT => tensor.getStorage.asInstanceOf[Array[Float]]
//  }

}
