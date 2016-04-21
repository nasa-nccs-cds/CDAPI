package nasa.nccs.cdapi.tensors
import scala.collection.mutable.ListBuffer
import java.nio._

import ucar.ma2
import ucar.ma2.DataType


object CDArray {
  def factory(shape: Array[Int], storage: Array[AnyVal]): CDArray = {
    return factory( CDIndex.factory(shape), storage)
  }

  def factory( index: CDIndex, storage: Array[AnyVal] ): CDArray = {
    storage.head.getClass.getSimpleName match {
      case "float" => return CDArrayFloat.factory( index, storage.asInstanceOf[Array[Float]] )
      case "int" => return CDArrayInt.factory( index, storage.asInstanceOf[Array[Int]] )
      case "byte" => return CDCDArrayByte.factory( index, storage.asInstanceOf[Array[Byte]] )
      case x => throw new Exception("Cant use this method for datatype " + x )
    }
  }
}

abstract class CDArray( val dataType: ma2.DataType, val indexCalc: CDIndex)  {

  def rank = indexCalc.rank


  def getNewIndex: CDIndex = {
    new CDIndex( this.indexCalc ).initIteration()
  }

  def getStorage: Array[AnyVal]

  def getDataType: ma2.DataType = {
    return this.dataType
  }

  def getIndex: CDIndex = {
    return new CDIndex( this.indexCalc )
  }

  def getIterator: CDIterator = {
    return new CDIterator( indexCalc )
  }

  def getRank: Int = {
    return rank
  }

  def getShape: Array[Int] = {
    return indexCalc.getShape
  }

  def getSize: Long = {
    return indexCalc.getSize
  }

  def getSizeBytes: Long = {
    return indexCalc.getSize * dataType.getSize
  }

  def getRangeIterator(ranges: List[ma2.Range] ): CDIterator = {
    return section(ranges).getIterator
  }

  def getElementType: Nothing

  def getStorage: Nothing

  protected def copyFrom1DJavaArray(iter: Nothing, javaArray: Nothing)

  protected def copyTo1DJavaArray(iter: Nothing, javaArray: Nothing)

  protected def createView( index: CDIndex ): CDArray

  def section(ranges: List[ma2.Range]): CDArray = {
    return createView(indexCalc.section(ranges))
  }

  def createRanges( origin: Array[Int], shape: Array[Int], strideOpt: Option[Array[Int]] = None ): List[ma2.Range] = {
    val strides: Array[Int] = strideOpt match {
      case Some(stride_array) => stride_array
      case None => Array.fill[Int](origin.length)(1)
    }
    val rangeSeq = for (i <- (0 until origin.length) ) yield {
      if (shape(i) < 0) ma2.Range.VLEN
      else new ma2.Range(origin(i), origin(i) + strides(i) * shape(i) - 1, strides(i))
    }
    rangeSeq.toList
  }


  def section(origin: Array[Int], shape: Array[Int], strideOpt: Option[Array[Int]]=None): CDArray = createView(indexCalc.section(createRanges(origin,shape,strideOpt)))


  def slice(dim: Int, value: Int): CDArray = {
    val origin: Array[Int] = new Array[Int](rank)
    val shape: Array[Int] = getShape
    origin(dim) = value
    shape(dim) = 1
    section(origin, shape).reduce(dim)
  }

  def getDataAsByteBuffer: ByteBuffer = {
    throw new Exception( "Unimplemented method")
  }


  def flip(dim: Int): CDArray = {
    return createView(indexCalc.flip(dim))
  }

  def transpose(dim1: Int, dim2: Int): CDArray = {
    return createView(indexCalc.transpose(dim1, dim2))
  }

  def permute(dims: Array[Int]): CDArray = {
    return createView(indexCalc.permute(dims))
  }

  def reshape(shape: Array[Int]): CDArray = {
    val result: Array = Array.factory(this.getDataType, shape)
    if (result.getSize != getSize) throw new Nothing("reshape arrays must have same total size")
    Array.arraycopy(this, 0, result, 0, getSize.toInt)
    return result
  }

  def reshapeNoCopy(shape: Array[Int]): CDArray = {
    val result: Array = Array.factory(this.getDataType, shape, getStorage)
    if (result.getSize != getSize) throw new Nothing("reshape arrays must have same total size")
    return result
  }

  def reduce: CDArray = {
    val ri: CDIndex = indexCalc.reduce
    if (ri eq indexCalc) return this
    return createView(ri)
  }

  def reduce(dim: Int): CDArray = {
    return createView(indexCalc.reduce(dim))
  }

  def isVlen: Boolean = {
    return false
  }

  def getFloat(ima: Nothing): Float

  def setFloat(ima: Nothing, value: Float)

  def getInt(ima: Nothing): Int

  def setInt(ima: Nothing, value: Int)

  def getByte(ima: Nothing): Byte

  def setByte(ima: Nothing, value: Byte)

  def getFloat(elem: Int): Float

  def setFloat(elem: Int, `val`: Float)

  def getInt(elem: Int): Int

  def setInt(elem: Int, value: Int)

  def getByte(elem: Int): Byte

  def setByte(elem: Int, value: Byte)

//
//  private var ii: Nothing = null
//
//  def hasNext: Boolean = {
//    if (null == ii) ii = getIndexIterator
//    return ii.hasNext
//  }
//
//  def next: Nothing = {
//    return ii.getObjectNext
//  }
//
//  def nextFloat: Float = {
//    return ii.getFloatNext
//  }
//
//  def nextByte: Byte = {
//    return ii.getByteNext
//  }
//
//  def nextShort: Short = {
//    return ii.getShortNext
//  }
//
//  def nextInt: Int = {
//    return ii.getIntNext
//  }
//  def resetLocalIterator {
//    ii = null
//  }
}



class CDArrayInt( index: CDIndex, var storage: Array[Int] ) extends CDArray( ma2.DataType.INT, index ) {

  protected def createView(index: Nothing): Nothing = {
    return CDArrayInt.factory(index, isUnsigned, storage)
  }

  def getStorage: Nothing = {
    return storage
  }

  protected def copyFrom1DJavaArray(iter: Nothing, javaArray: Nothing) {
    val ja: Array[Int] = javaArray.asInstanceOf[Array[Int]]
    for (aJa <- ja) iter.setIntNext(aJa)
  }

  protected def copyTo1DJavaArray(iter: Nothing, javaArray: Nothing) {
    val ja: Array[Int] = javaArray.asInstanceOf[Array[Int]]
    var i: Int = 0
    while (i < ja.length) {
      ja(i) = iter.getIntNext
      ({
        i += 1; i - 1
      })
    }
  }

  def getDataAsByteBuffer: Nothing = {
    return getDataAsByteBuffer(null)
  }

  def getDataAsByteBuffer(order: Nothing): Nothing = {
    val bb: Nothing = super.getDataAsByteBuffer((4 * getSize).asInstanceOf[Int], order)
    val ib: Nothing = bb.asIntBuffer
    ib.put(get1DJavaArray(classOf[Int]).asInstanceOf[Array[Int]])
    return bb
  }

  def getElementType: Nothing = {
    return classOf[Int]
  }

  def get(i: CDIndex): Int = {
    return storage(i.currentElement)
  }

  def set(i: CDIndex, value: Int) {
    storage(i.currentElement) = value
  }

  def getFloat(i: CDIndex): Float = {
    val `val`: Int = storage(i.currentElement)
    return (if (isUnsigned) ma2.DataType.unsignedIntToLong(`val`)
    else `val`).toFloat
  }

  def setFloat(i: CDIndex, value: Float) {
    storage(i.currentElement) = value.toInt
  }


  def getInt(i: CDIndex): Int = {
    return storage(i.currentElement)
  }

  def setInt(i: CDIndex, value: Int) {
    storage(i.currentElement) = value
  }


  def getByte(i: CDIndex): Byte = {
    return storage(i.currentElement).toByte
  }

  def setByte(i: CDIndex, value: Byte) {
    storage(i.currentElement) = value.toInt
  }

  def getFloat(index: Int): Float = {
    val `val`: Int = storage(index)
    return (if (isUnsigned) ma2.DataType.unsignedIntToLong(`val`)
    else `val`).toFloat
  }

  def setFloat(index: Int, value: Float) {
    storage(index) = value.toInt
  }


  def getInt(index: Int): Int = {
    return storage(index)
  }

  def setInt(index: Int, value: Int) {
    storage(index) = value
  }

}

class CDArrayFloat( index: CDIndex, var storage: Array[Float] ) extends CDArray( ma2.DataType.FLOAT, index ) {


  protected def createView(index: CDIndex ): CDArrayFloat = {
    return CDArrayFloat.factory(index, storage)
  }

  def getStorage: Array[AnyVal] = {
    return storage
  }

  protected def copyFrom1DJavaArray(iter: Nothing, javaArray: Nothing) {
    val ja: Array[Float] = javaArray.asInstanceOf[Array[Float]]
    for (aJa <- ja) iter.setFloatNext(aJa)
  }

  protected def copyTo1DJavaArray(iter: Nothing, javaArray: Nothing) {
    val ja: Array[Float] = javaArray.asInstanceOf[Array[Float]]
    var i: Int = 0
    while (i < ja.length) {
      ja(i) = iter.getFloatNext
      ({
        i += 1; i - 1
      })
    }
  }

  def getDataAsByteBuffer: Nothing = {
    val bb: Nothing = ByteBuffer.allocate((4 * getSize).asInstanceOf[Int])
    val ib: Nothing = bb.asFloatBuffer
    ib.put(get1DJavaArray(ma2.DataType.FLOAT).asInstanceOf[Array[Float]])
    return bb
  }

  def getElementType: Nothing = {
    return classOf[Float]
  }

  def get(i: CDIndex): Float = {
    return storage(i.currentElement)
  }

  def set(i: CDIndex, value: Float) {
    storage(i.currentElement) = value
  }

  def getFloat(i: CDIndex): Float = {
    return storage(i.currentElement)
  }

  def setFloat(i: CDIndex, value: Float) {
    storage(i.currentElement) = value
  }

  def getInt(i: CDIndex): Int = {
    return storage(i.currentElement).toInt
  }

  def setInt(i: CDIndex, value: Int) {
    storage(i.currentElement) = value.toFloat
  }

  def getByte(i: CDIndex): Byte = {
    return storage(i.currentElement).toByte
  }

  def setByte(i: CDIndex, value: Byte) {
    storage(i.currentElement) = value.toFloat
  }

  def getFloat(index: Int): Float = {
    return storage(index)
  }

  def setFloat(index: Int, value: Float) {
    storage(index) = value
  }

  def getInt(index: Int): Int = {
    return storage(index).toInt
  }

  def setInt(index: Int, value: Int) {
    storage(index) = value.toFloat
  }

  def getByte(index: Int): Byte = {
    return storage(index).toByte
  }

  def setByte(index: Int, value: Byte) {
    storage(index) = value.toFloat
  }

}

class CDArrayByte( index: CDIndex, var storage: Array[Byte] ) extends CDArray( ma2.DataType.Byte, index ) {

  protected def createView(index: CDIndex): CDArrayByte = {
    return CDArrayByte.factory(index, storage)
  }

  def getStorage: Nothing = {
    return storage
  }

  protected def copyFrom1DJavaArray(iter: Nothing, javaArray: Nothing) {
    val ja: Array[Byte] = javaArray.asInstanceOf[Array[Byte]]
    for (aJa <- ja) iter.setByteNext(aJa)
  }

  protected def copyTo1DJavaArray(iter: Nothing, javaArray: Nothing) {
    val ja: Array[Byte] = javaArray.asInstanceOf[Array[Byte]]
    var i: Int = 0
    while (i < ja.length) {
      ja(i) = iter.getByteNext
      ({
        i += 1; i - 1
      })
    }
  }

  @Override def getDataAsByteBuffer: Nothing = {
    return getDataAsByteBuffer(null)
  }

  def getDataAsByteBuffer(order: Nothing): Nothing = {
    return ByteBuffer.wrap(get1DJavaArray(getDataType).asInstanceOf[Array[Byte]])
  }

  def getElementType: Nothing = {
    return classOf[Byte]
  }

  def get(i: CDIndex): Byte = {
    return storage(i.currentElement)
  }

  def set(i: CDIndex, value: Byte) {
    storage(i.currentElement) = value
  }

  def getFloat(i: CDIndex): Float = {
    val `val`: Byte = storage(i.currentElement)
    return (if (isUnsigned) ma2.DataType.unsignedByteToShort(`val`)
    else `val`).toFloat
  }

  def setFloat(i: CDIndex, value: Float) {
    storage(i.currentElement) = value.toByte
  }

  def getInt(i: CDIndex): Int = {
    val `val`: Byte = storage(i.currentElement)
    return (if (isUnsigned) ma2.DataType.unsignedByteToShort(`val`)
    else `val`).toInt
  }

  def setInt(i: CDIndex, value: Int) {
    storage(i.currentElement) = value.toByte
  }



  def getByte(i: CDIndex): Byte = {
    return storage(i.currentElement)
  }

  def setByte(i: CDIndex, value: Byte) {
    storage(i.currentElement) = value
  }


  def getFloat(index: Int): Float = {
    val `val`: Byte = storage(index)
    return (if (isUnsigned) ma2.DataType.unsignedByteToShort(`val`)
    else `val`).toFloat
  }

  def setFloat(index: Int, value: Float) {
    storage(index) = value.toByte
  }

  def getInt(index: Int): Int = {
    val `val`: Byte = storage(index)
    return (if (isUnsigned) ma2.DataType.unsignedByteToShort(`val`)
    else `val`).toInt
  }

  def setInt(index: Int, value: Int) {
    storage(index) = value.toByte
  }

  def getByte(index: Int): Byte = {
    return storage(index)
  }

  def setByte(index: Int, value: Byte) {
    storage(index) = value
  }


}



