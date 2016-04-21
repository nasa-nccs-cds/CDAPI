package nasa.nccs.cdapi.tensors

import javax.annotation.Nonnull
import java.util.Arrays
import java.nio._
import ucar.ma2

object CDArray {
  def factory(dataType: Nothing, shape: Array[Int]): CDArray = {
    return factory(dataType, Index.factory(shape), null)
  }

  def factory(dataType: Nothing, shape: Array[Int], storage: Nothing): CDArray = {
    return factory(dataType, Index.factory(shape), storage)
  }

  def factory(dtype: Nothing, index: Nothing, storage: Nothing): CDArray = {
    dtype match {
      case DOUBLE =>
        return ArrayDouble.factory(index, storage.asInstanceOf[Array[Double]])
      case FLOAT =>
        return ArrayFloat.factory(index, storage.asInstanceOf[Array[Float]])
      case CHAR =>
        return ArrayChar.factory(index, storage.asInstanceOf[Array[Char]])
      case BOOLEAN =>
        return ArrayBoolean.factory(index, storage.asInstanceOf[Array[Boolean]])
      case ENUM4 =>
      case UINT =>
      case INT =>
        return ArrayInt.factory(index, dtype.isUnsigned, storage.asInstanceOf[Array[Int]])
      case ENUM2 =>
      case USHORT =>
      case SHORT =>
        return ArrayShort.factory(index, dtype.isUnsigned, storage.asInstanceOf[Array[Short]])
      case ENUM1 =>
      case UBYTE =>
      case BYTE =>
        return ArrayByte.factory(index, dtype.isUnsigned, storage.asInstanceOf[Array[Byte]])
      case ULONG =>
      case LONG =>
        return ArrayLong.factory(index, dtype.isUnsigned, storage.asInstanceOf[Array[Long]])
      case STRING =>
        return ArrayObject.factory(dtype, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
      case STRUCTURE =>
        return ArrayObject.factory(dtype, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
      case SEQUENCE =>
        return ArrayObject.factory(dtype, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
      case OPAQUE =>
        return ArrayObject.factory(dtype, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
    }
    throw new Nothing("Cant use this method for datatype " + dtype)
  }

  def makeVlenArray(shape: Array[Int], @Nonnull storage: Array[Array]): CDArray = {
    val index: Nothing = Index.factory(shape)
    return ArrayObject.factory(storage(0).getDataType, storage(0).getClass, true, index, storage)
  }

  def makeObjectArray(dtype: Nothing, classType: Nothing, shape: Array[Int], storage: Nothing): Array = {
    val index: Nothing = Index.factory(shape)
    return ArrayObject.factory(dtype, classType, false, index, storage.asInstanceOf[Array[Nothing]])
  }

  def factoryConstant(dtype: Nothing, shape: Array[Int], storage: Nothing): CDArray = {
    val index: Nothing = new Nothing(shape)
    dtype match {
      case BOOLEAN =>
        return new Nothing(index, storage.asInstanceOf[Array[Boolean]])
      case BYTE =>
        return new Nothing(index, false, storage.asInstanceOf[Array[Byte]])
      case CHAR =>
        return new Nothing(index, storage.asInstanceOf[Array[Char]])
      case SHORT =>
        return new Nothing(index, false, storage.asInstanceOf[Array[Short]])
      case INT =>
        return new Nothing(index, false, storage.asInstanceOf[Array[Int]])
      case LONG =>
        return new Nothing(index, false, storage.asInstanceOf[Array[Long]])
      case FLOAT =>
        return new Nothing(index, storage.asInstanceOf[Array[Float]])
      case DOUBLE =>
        return new Nothing(index, storage.asInstanceOf[Array[Double]])
      case ENUM1 =>
      case UBYTE =>
        return new Nothing(index, true, storage.asInstanceOf[Array[Byte]])
      case ENUM2 =>
      case USHORT =>
        return new Nothing(index, true, storage.asInstanceOf[Array[Short]])
      case ENUM4 =>
      case UINT =>
        return new Nothing(index, true, storage.asInstanceOf[Array[Int]])
      case ULONG =>
        return new Nothing(index, true, storage.asInstanceOf[Array[Long]])
      case STRING =>
        return new Nothing(dtype, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
      case STRUCTURE =>
        return new Nothing(dtype, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
      case SEQUENCE =>
        return new Nothing(dtype, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
      case OPAQUE =>
        return new Nothing(dtype, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
      case _ =>
        return ArrayObject.factory(DataType.OBJECT, classOf[Nothing], false, index, storage.asInstanceOf[Array[Nothing]])
    }
  }

  def makeFromJavaArray(javaArray: Nothing): CDArray = {
    return makeFromJavaArray(javaArray, false)
  }

  def makeFromJavaArray(javaArray: Nothing, isUnsigned: Boolean): CDArray = {
    var rank_: Int = 0
    var componentType: Nothing = javaArray.getClass
    while (componentType.isArray) {
      {
        rank_ += 1
        componentType = componentType.getComponentType
      }
    }
    var count: Int = 0
    val shape: Array[Int] = new Array[Int](rank_)
    var jArray: Nothing = javaArray
    var cType: Nothing = jArray.getClass
    while (cType.isArray) {
      {
        shape(({
          count += 1; count - 1
        })) = java.lang.reflect.Array.getLength(jArray)
        jArray = java.lang.reflect.Array.get(jArray, 0)
        cType = jArray.getClass
      }
    }
    val dtype: Nothing = DataType.getType(componentType, isUnsigned)
    val aa: Array = factory(dtype, shape)
    val aaIter: Nothing = aa.getIndexIterator
    reflectArrayCopyIn(javaArray, aa, aaIter)
    return aa
  }

  private def reflectArrayCopyIn(jArray: Nothing, aa: CDArray, aaIter: Nothing) {
    val cType: Nothing = jArray.getClass.getComponentType
    if (cType.isPrimitive) {
      aa.copyFrom1DJavaArray(aaIter, jArray)
    }
    else {
      var i: Int = 0
      while (i < java.lang.reflect.Array.getLength(jArray)) {
        reflectArrayCopyIn(java.lang.reflect.Array.get(jArray, i), aa, aaIter)
        ({
          i += 1; i - 1
        })
      }
    }
  }

  private def reflectArrayCopyOut(jArray: Nothing, aa: Array, aaIter: Nothing) {
    val cType: Nothing = jArray.getClass.getComponentType
    if (!cType.isArray) {
      aa.copyTo1DJavaArray(aaIter, jArray)
    }
    else {
      var i: Int = 0
      while (i < java.lang.reflect.Array.getLength(jArray)) {
        reflectArrayCopyOut(java.lang.reflect.Array.get(jArray, i), aa, aaIter)
        ({
          i += 1; i - 1
        })
      }
    }
  }

  def arraycopy(arraySrc: Array, srcPos: Int, arrayDst: Array, dstPos: Int, len: Int) {
    if (arraySrc.isConstant) {
      val d: Double = arraySrc.getDouble(0)
      var i: Int = dstPos
      while (i < dstPos + len) {
        arrayDst.setDouble(i, d)
        ({
          i += 1; i - 1
        })
      }
      return
    }
    val src: Nothing = arraySrc.get1DJavaArray(arraySrc.getDataType)
    val dst: Nothing = arrayDst.getStorage
    System.arraycopy(src, srcPos, dst, dstPos, len)
  }

  def makeArray(dtype: Nothing, npts: Int, start: Double, incr: Double): CDArray = {
    val result: CDArray = CDArray.factory(dtype, Array[Int](npts))
    val dataI: Nothing = result.getIndexIterator
    var i: Int = 0
    while (i < npts) {
      {
        val `val`: Double = start + i * incr
        dataI.setDoubleNext(`val`)
      }
      ({
        i += 1; i - 1
      })
    }
    return result
  }

  @throws[NumberFormatException]
  def makeArray(dtype: Nothing, stringValues: Nothing): CDArray = {
    val result: CDArray = CDArray.factory(dtype, Array[Int](stringValues.size))
    val dataI: Nothing = result.getIndexIterator
    import scala.collection.JavaConversions._
    for (s <- stringValues) {
      if (dtype eq DataType.STRING) {
        dataI.setObjectNext(s)
      }
      else if (dtype eq DataType.LONG) {
        if (dtype.isUnsigned) {
          val biggy: Nothing = new Nothing(s)
          val convert: Long = biggy.longValue
          dataI.setLongNext(biggy.longValue)
        }
        else {
          val `val`: Long = Long.parseLong(s)
          dataI.setLongNext(`val`)
        }
      }
      else {
        val `val`: Double = Double.parseDouble(s)
        dataI.setDoubleNext(`val`)
      }
    }
    return result
  }

  @throws[NumberFormatException]
  def makeArray(dtype: Nothing, stringValues: Array[Nothing]): CDArray = {
    return makeArray(dtype, Arrays.asList(stringValues))
  }

  def makeArrayRankPlusOne(org: Array): CDArray = {
    val shape: Array[Int] = new Array[Int](org.getRank + 1)
    System.arraycopy(org.getShape, 0, shape, 1, org.getRank)
    shape(0) = 1
    return factory(org.getDataType, shape, org.getStorage)
  }

  def factory(dtype: Nothing, shape: Array[Int], bb: Nothing): CDArray = {
    var size: Int = 0
    var result: Array = null
    dtype match {
      case ENUM1 =>
      case UBYTE =>
      case BYTE =>
        size = bb.limit
        if (shape == null) shape = Array[Int](size)
        result = factory(dtype, shape)
        var i: Int = 0
        while (i < size) {
          result.setByte(i, bb.get(i))
          ({
            i += 1; i - 1
          })
        }
        return result
      case CHAR =>
        size = bb.limit
        if (shape == null) shape = Array[Int](size)
        result = factory(dtype, shape)
        var i: Int = 0
        while (i < size) {
          result.setByte(i, bb.get(i))
          ({
            i += 1; i - 1
          })
        }
        return result
      case ENUM2 =>
      case USHORT =>
      case SHORT =>
        val sb: Nothing = bb.asShortBuffer
        size = sb.limit
        if (shape == null) shape = Array[Int](size)
        result = factory(dtype, shape)
        var i: Int = 0
        while (i < size) {
          result.setShort(i, sb.get(i))
          ({
            i += 1; i - 1
          })
        }
        return result
      case ENUM4 =>
      case UINT =>
      case INT =>
        val ib: Nothing = bb.asIntBuffer
        size = ib.limit
        if (shape == null) shape = Array[Int](size)
        result = factory(dtype, shape)
        var i: Int = 0
        while (i < size) {
          result.setInt(i, ib.get(i))
          ({
            i += 1; i - 1
          })
        }
        return result
      case ULONG =>
      case LONG =>
        val lb: Nothing = bb.asLongBuffer
        size = lb.limit
        if (shape == null) shape = Array[Int](size)
        result = factory(dtype, shape)
        var i: Int = 0
        while (i < size) {
          result.setLong(i, lb.get(i))
          ({
            i += 1; i - 1
          })
        }
        return result
      case FLOAT =>
        val ffb: Nothing = bb.asFloatBuffer
        size = ffb.limit
        if (shape == null) shape = Array[Int](size)
        result = factory(dtype, shape)
        var i: Int = 0
        while (i < size) {
          result.setFloat(i, ffb.get(i))
          ({
            i += 1; i - 1
          })
        }
        return result
      case DOUBLE =>
        val db: Nothing = bb.asDoubleBuffer
        size = db.limit
        if (shape == null) shape = Array[Int](size)
        result = factory(dtype, shape)
        var i: Int = 0
        while (i < size) {
          result.setDouble(i, db.get(i))
          ({
            i += 1; i - 1
          })
        }
        return result
    }
    throw new Nothing("" + dtype)
  }
}

abstract class CDArray {
  final protected var dataType: Nothing = null
  final protected var indexCalc: Nothing = null
  final protected var rank: Int = 0

  def this(dataType: Nothing, shape: Array[Int]) {
    this()
    this.dataType = dataType
    this.rank = shape.length
    this.indexCalc = Index.factory(shape)
  }

  def this(dataType: Nothing, index: Nothing) {
    this()
    this.dataType = dataType
    this.rank = index.getRank
    this.indexCalc = index
  }

  def getDataType: Nothing = {
    return this.dataType
  }

  def getIndex: Nothing = {
    return indexCalc.clone.asInstanceOf[Nothing]
  }

  def getIndexIterator: Nothing = {
    return indexCalc.getIndexIterator(this)
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
    val dtype: Nothing = DataType.getType(this)
    return indexCalc.getSize * dtype.getSize
  }

  @throws[InvalidRangeException]
  def getRangeIterator(ranges: Nothing): Nothing = {
    return section(ranges).getIndexIterator
  }

  def getElementType: Nothing

  def getStorage: Nothing

  protected def copyFrom1DJavaArray(iter: Nothing, javaArray: Nothing)

  protected def copyTo1DJavaArray(iter: Nothing, javaArray: Nothing)

  protected def createView(index: Nothing): Array

  @throws[InvalidRangeException]
  def section(ranges: Nothing): Array = {
    return createView(indexCalc.section(ranges))
  }

  @throws[InvalidRangeException]
  def section(origin: Array[Int], shape: Array[Int]): Array = {
    return section(origin, shape, null)
  }

  @throws[InvalidRangeException]
  def section(origin: Array[Int], shape: Array[Int], stride: Array[Int]): Array = {
    val ranges: Nothing = new Nothing(origin.length)
    if (stride == null) {
      stride = new Array[Int](origin.length)
      var i: Int = 0
      while (i < stride.length) {
        stride(i) = 1
        ({
          i += 1; i - 1
        })
      }
    }
    var i: Int = 0
    while (i < origin.length) {
      ranges.add(new Nothing(origin(i), origin(i) + stride(i) * shape(i) - 1, stride(i)))
      ({
        i += 1; i - 1
      })
    }
    return createView(indexCalc.section(ranges))
  }

  @throws[InvalidRangeException]
  def sectionNoReduce(ranges: Nothing): Array = {
    return createView(indexCalc.sectionNoReduce(ranges))
  }

  @throws[InvalidRangeException]
  def sectionNoReduce(origin: Array[Int], shape: Array[Int], stride: Array[Int]): Array = {
    val ranges: Nothing = new Nothing(origin.length)
    if (stride == null) {
      stride = new Array[Int](origin.length)
      var i: Int = 0
      while (i < stride.length) {
        stride(i) = 1
        ({
          i += 1; i - 1
        })
      }
    }
    var i: Int = 0
    while (i < origin.length) {
      {
        if (shape(i) < 0) ranges.add(Range.VLEN)
        else ranges.add(new Nothing(origin(i), origin(i) + stride(i) * shape(i) - 1, stride(i)))
      }
      ({
        i += 1; i - 1
      })
    }
    return createView(indexCalc.sectionNoReduce(ranges))
  }

  def slice(dim: Int, value: Int): Array = {
    val origin: Array[Int] = new Array[Int](rank)
    val shape: Array[Int] = getShape
    origin(dim) = value
    shape(dim) = 1
    try {
      return sectionNoReduce(origin, shape, null).reduce(dim)
    }
    catch {
      case e: Nothing => {
        throw new Nothing
      }
    }
  }

  def copy: CDArray = {
    val newA: CDArray = CDArray.factory(getDataType, getShape)
    MAMath.copy(newA, this)
    return newA
  }

  def get1DJavaArray(wantType: Nothing): Nothing = {
    if (wantType eq getDataType) {
      if (indexCalc.isFastIterator) return getStorage
      else return copyTo1DJavaArray
    }
    val newA: Array = Array.factory(wantType, getShape)
    MAMath.copy(newA, this)
    return newA.getStorage
  }

  def get1DJavaArray(wantType: Nothing): Nothing = {
    val want: Nothing = DataType.getType(wantType, isUnsigned)
    return get1DJavaArray(want)
  }

  def getDataAsByteBuffer: Nothing = {
    throw new Nothing
  }

  def getDataAsByteBuffer(order: Nothing): Nothing = {
    throw new Nothing
  }

  def getDataAsByteBuffer(capacity: Int, order: Nothing): Nothing = {
    val bb: Nothing = ByteBuffer.allocate(capacity)
    if (order != null) bb.order(order)
    return bb
  }

  def copyTo1DJavaArray: Nothing = {
    val newA: Array = copy
    return newA.getStorage
  }

  def copyToNDJavaArray: Nothing = {
    var javaArray: Nothing = null
    try {
      javaArray = java.lang.reflect.Array.newInstance(getElementType, getShape)
    }
    catch {
      case e: Nothing => {
        throw new Nothing(e)
      }
    }
    val iter: Nothing = getIndexIterator
    Array.reflectArrayCopyOut(javaArray, this, iter)
    return javaArray
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
    val ri: Nothing = indexCalc.reduce
    if (ri eq indexCalc) return this
    return createView(ri)
  }

  def reduce(dim: Int): CDArray = {
    return createView(indexCalc.reduce(dim))
  }

  def isUnsigned: Boolean = {
    return dataType.isUnsigned
  }

  def isConstant: Boolean = {
    return indexCalc.isInstanceOf[Nothing]
  }

  def isVlen: Boolean = {
    return false
  }

  def getDouble(ima: Nothing): Double

  def setDouble(ima: Nothing, value: Double)

  def getFloat(ima: Nothing): Float

  def setFloat(ima: Nothing, value: Float)

  def getLong(ima: Nothing): Long

  def setLong(ima: Nothing, value: Long)

  def getInt(ima: Nothing): Int

  def setInt(ima: Nothing, value: Int)

  def getShort(ima: Nothing): Short

  def setShort(ima: Nothing, value: Short)

  def getByte(ima: Nothing): Byte

  def setByte(ima: Nothing, value: Byte)

  def getChar(ima: Nothing): Char

  def setChar(ima: Nothing, value: Char)

  def getBoolean(ima: Nothing): Boolean

  def setBoolean(ima: Nothing, value: Boolean)

  def getObject(ima: Nothing): Nothing

  def setObject(ima: Nothing, value: Nothing)

  def getDouble(elem: Int): Double

  def setDouble(elem: Int, `val`: Double)

  def getFloat(elem: Int): Float

  def setFloat(elem: Int, `val`: Float)

  def getLong(elem: Int): Long

  def setLong(elem: Int, value: Long)

  def getInt(elem: Int): Int

  def setInt(elem: Int, value: Int)

  def getShort(elem: Int): Short

  def setShort(elem: Int, value: Short)

  def getByte(elem: Int): Byte

  def setByte(elem: Int, value: Byte)

  def getChar(elem: Int): Char

  def setChar(elem: Int, value: Char)

  def getBoolean(elem: Int): Boolean

  def setBoolean(elem: Int, value: Boolean)

  def getObject(elem: Int): Nothing

  def setObject(elem: Int, value: Nothing)

  def toString: Nothing = {
    val sbuff: Nothing = new Nothing
    val ii: Nothing = getIndexIterator
    while (ii.hasNext) {
      {
        val data: Nothing = ii.getObjectNext
        sbuff.append(data)
        sbuff.append(" ")
      }
    }
    return sbuff.toString
  }

  def shapeToString: Nothing = {
    val shape: Array[Int] = getShape
    if (shape.length == 0) return ""
    val sb: Nothing = new Nothing
    sb.append('(')
    var i: Int = 0
    while (i < shape.length) {
      {
        val s: Int = shape(i)
        if (i > 0) sb.append(",")
        sb.append(s)
      }
      ({
        i += 1; i - 1
      })
    }
    sb.append(')')
    return sb.toString
  }

  private var ii: Nothing = null

  def hasNext: Boolean = {
    if (null == ii) ii = getIndexIterator
    return ii.hasNext
  }

  def next: Nothing = {
    return ii.getObjectNext
  }

  def nextDouble: Double = {
    return ii.getDoubleNext
  }

  def nextFloat: Float = {
    return ii.getFloatNext
  }

  def nextByte: Byte = {
    return ii.getByteNext
  }

  def nextShort: Short = {
    return ii.getShortNext
  }

  def nextInt: Int = {
    return ii.getIntNext
  }

  def nextLong: Long = {
    return ii.getLongNext
  }

  def nextChar: Char = {
    return ii.getCharNext
  }

  def nextBoolean: Boolean = {
    return ii.getBooleanNext
  }

  def resetLocalIterator {
    ii = null
  }
}

