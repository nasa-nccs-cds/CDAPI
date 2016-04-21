package nasa.nccs.cdapi.tensors
import ucar.ma2
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
// Based on ucar.ma2.CDIndex, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

object CDIndex {
  val scalarCDIndexImmutable: CDIndex0D = new CDIndex0D()

  def factory(shape: Array[Int]): CDIndex = {
    val rank: Int = shape.length
    rank match {
      case 0 =>
        return new CDIndex0D;
      case 1 =>
        return new CDIndex1D(shape)
      case 2 =>
        return new CDIndex2D(shape)
      case 3 =>
        return new CDIndex3D(shape)
      case 4 =>
        return new CDIndex4D(shape)
      case 5 =>
        return new CDIndex5D(shape)
      case _ =>
        return new CDIndex(shape)
    }
  }

  def computeSize(shape: Array[Int]): Long = {
    var product: Long = 1
    for (aShape <- shape; if (aShape >= 0)) product *= aShape
    return product
  }

  def computeStrides( shape: Array[Int] ): Array[Int] = {
    var product: Long = 1
    var strides = for (ii <- (shape.length - 1 to 0 by -1); thisDim = shape(ii) ) yield if (thisDim >= 0) { product *= thisDim; product.toInt } else { 0 }
    return strides.toArray
  }
}

trait TIndexIterator {
  def hasNext: Boolean
  def getFloatNext: Float
  def setFloatNext(value: Float)
  def getFloatCurrent: Float
  def setFloatCurrent(value: Float)
  def next: Float
  def getCurrentCounter: Array[Int]
}

private class IteratorImpl extends TIndexIterator {
private var count: Int = 0
private var currElement: Int = 0
private var counter: CDIndex = null
private var maa: CDArray = null

class IteratorImpl( _maa: CDArray  ) {
  val maa = _maa;
  val counter = new CDIndex(maa.getIndex).initIteration

def hasNext: Boolean = {
    return count < counter.size
  }


  def getCurrentCounter: Array[Int] = {
    return counter.getCurrentCounter
  }

  def next: Nothing = {
    count += 1
    currElement = counter.incr
    return maa.getObject(currElement)
  }

  def getFloatCurrent: Float = {
    return maa.getFloat(currElement)
  }

  def getFloatNext: Float = {
    count += 1
    currElement = counter.incr
    return maa.getFloat(currElement)
  }

  def setFloatCurrent(value: Float) {
    maa.setFloat(currElement, value)
  }

  def setFloatNext(value: Float) {
    count += 1
    currElement = counter.incr
    maa.setFloat(currElement, value)
  }

}

class CDIndex( val shape: Array[Int], val stride: Array[Int], val offset: Int = 0 ) {
  val rank: Int = shape.length
  val size: Long = CDIndex.computeSize(shape)
  val hasvlen: Boolean = (shape.length > 0 && shape(shape.length - 1) < 0)
  private var fastIterator: Boolean = ( offset == 0 )
  protected var current: Array[Int] = new Array[Int](rank)

  def this( shape: Array[Int] ) = {
    this( shape, CDIndex.computeStrides(shape) )
  }

  def this( index: CDIndex ) = {
    this( index.shape, index.stride, index.offset )
  }

  protected def precalc() {}

  def initIteration() {
    if (rank > 0) {
      current(rank - 1) = -1    // avoid "if first" on every incr.
      precalc()
    }
  }

  def flip(index: Int): CDIndex = {
    if ((index < 0) || (index >= rank)) throw new Nothing
    val i: CDIndex = this.clone.asInstanceOf[CDIndex]
    if (shape(index) >= 0) {
      i.offset += stride(index) * (shape(index) - 1)
      i.stride(index) = -stride(index)
    }
    i.fastIterator = false
    i.precalc
    return i
  }

  def section( ranges: List[ma2.Range] ): CDIndex = {
    assert(ranges.size == rank, "Bad ranges [] length")
    for( ii <-(0 until rank); r = ranges(ii); if ((r != null) && (r != ma2.Range.VLEN)) ) {
      assert ((r.first >= 0) && (r.first < shape(ii)), "Bad range starting value at index " + ii + " == " + r.first)
      assert ((r.last >= 0) && (r.last < shape(ii)), "Bad range ending value at index " + ii + " == " + r.last)
    }
    var reducedRank: Int = rank
    for (r <- ranges) {
      if ((r != null) && (r.length eq 1)) ({
        reducedRank -= 1; reducedRank + 1
      })
    }
    val newindex: CDIndex = CDIndex.factory(rank)
    newindex.offset = offset
    for( ii <-(0 until rank); r = ranges(ii) ) {
      if (r == null) {
        newindex.shape(ii) = shape(ii)
        newindex.stride(ii) = stride(ii)
      }
      else {
        newindex.shape(ii) = r.length
        newindex.stride(ii) = stride(ii) * r.stride
        newindex.offset += stride(ii) * r.first
      }
    }
    newindex.size = CDIndex.computeSize(newindex.shape)
    newindex.fastIterator = fastIterator && (newindex.size == size)
    newindex.precalc
    return newindex
  }

  def reduce: CDIndex = {
    val c: CDIndex = this
    for( ii <-(0 until rank); if (shape(ii) == 1) ) {
        val newc: CDIndex = c.reduce(ii)
        return newc.reduce
    }
    return c
  }

  def reduce(dim: Int): CDIndex = {
    assert((dim >= 0) && (dim < rank), "illegal reduce dim " + dim )
    assert( (shape(dim) == 1), "illegal reduce dim " + dim + " : length != 1" )
    val newindex: CDIndex = CDIndex.factory(rank - 1)
    newindex.offset = offset
    var count: Int = 0
    for( ii <-(0 until rank); if (ii != dim) ) {
        newindex.shape(count) = shape(ii)
        newindex.stride(count) = stride(ii)
        count += 1
    }
    newindex.size = CDIndex.computeSize(newindex.shape)
    newindex.fastIterator = fastIterator
    newindex.precalc
    return newindex
  }

  def transpose(index1: Int, index2: Int): CDIndex = {
    assert((index1 >= 0) && (index1 < rank), "illegal index in transpose " + index1 )
    assert((index2 >= 0) && (index2 < rank), "illegal index in transpose " + index1 )
    val newCDIndex: CDIndex = this.clone.asInstanceOf[CDIndex]
    newCDIndex.stride(index1) = stride(index2)
    newCDIndex.stride(index2) = stride(index1)
    newCDIndex.shape(index1) = shape(index2)
    newCDIndex.shape(index2) = shape(index1)
    newCDIndex.fastIterator = false
    newCDIndex.precalc
    return newCDIndex
  }

  def permute(dims: Array[Int]): CDIndex = {
    assert( (dims.length == shape.length), "illegal shape in permute " + dims )
    for (dim <- dims) if ((dim < 0) || (dim >= rank)) throw new Exception( "illegal shape in permute " + dims )
    var isPermuted: Boolean = false
    val newCDIndex: CDIndex = this.clone.asInstanceOf[CDIndex]
    for( i <-(0 until dims.length) ) {
      newCDIndex.stride(i) = stride(dims(i))
      newCDIndex.shape(i) = shape(dims(i))
      if (i != dims(i)) isPermuted = true
    }
    newCDIndex.fastIterator = fastIterator && !isPermuted
    newCDIndex.precalc
    return newCDIndex
  }

  def getRank: Int = {
    return rank
  }

  def getShape: Array[Int] = shape

  def getShape(index: Int): Int = {
    return shape(index)
  }

  private[ma2] def getCDIndexIterator(maa: Nothing): Nothing = {
    if (fastIterator) return new Nothing(size, maa)
    else return new CDIndex#IteratorImpl(maa)
  }

  private[ma2] def getSlowCDIndexIterator(maa: Nothing): Nothing = {
    return new CDIndex#IteratorImpl(maa)
  }

  private[ma2] def getCDIndexIteratorFast(maa: Nothing): Nothing = {
    return new Nothing(size, maa)
  }

  private[ma2] def isFastIterator: Boolean = {
    return fastIterator
  }

  def getSize: Long = {
    return size
  }

  def currentElement: Int = {
    var value: Int = offset
    var ii: Int = 0
    while (ii < rank) {
      {
        if (shape(ii) < 0) break //todo: break is not supported
        value += current(ii) * stride(ii)
      }
      ({
        ii += 1; ii - 1
      })
    }
    return value
  }

  def getCurrentCounter: Array[Int] = {
    return current.clone
  }

  def setCurrentCounter(currElement: Int) {
    currElement -= offset
    var ii: Int = 0
    while (ii < rank) {
      {
        if (shape(ii) < 0) {
          current(ii) = -1
          break //todo: break is not supported
        }
        current(ii) = currElement / stride(ii)
        currElement -= current(ii) * stride(ii)
      }
      ({
        ii += 1; ii - 1
      })
    }
    set(current)
  }

  def incr: Int = {
    var digit: Int = rank - 1
    while (digit >= 0) {
      {
        if (shape(digit) < 0) {
          current(digit) = -1
          continue //todo: continue is not supported
        }
        current(digit) += 1
        if (current(digit) < shape(digit)) break //todo: break is not supported
        current(digit) = 0
        digit -= 1
      }
    }
    return currentElement
  }

  def set(index: Array[Int]): CDIndex = {
    if (index.length != rank) throw new Nothing
    if (rank == 0) return this
    val prefixrank: Int = (if (hasvlen) rank
    else rank - 1)
    System.arraycopy(index, 0, current, 0, prefixrank)
    if (hasvlen) current(prefixrank) = -1
    return this
  }

  def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Nothing
    if (shape(dim) >= 0) current(dim) = value
  }

  def set0(v: Int): CDIndex = {
    setDim(0, v)
    return this
  }

  def set1(v: Int): CDIndex = {
    setDim(1, v)
    return this
  }

  def set2(v: Int): CDIndex = {
    setDim(2, v)
    return this
  }

  def set3(v: Int): CDIndex = {
    setDim(3, v)
    return this
  }

  def set4(v: Int): CDIndex = {
    setDim(4, v)
    return this
  }

  def set5(v: Int): CDIndex = {
    setDim(5, v)
    return this
  }

  def set6(v: Int): CDIndex = {
    setDim(6, v)
    return this
  }

  def set(v0: Int): CDIndex = {
    setDim(0, v0)
    return this
  }

  def set(v0: Int, v1: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    return this
  }

  def set(v0: Int, v1: Int, v2: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    return this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    return this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    setDim(4, v4)
    return this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int, v5: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    setDim(4, v4)
    setDim(5, v5)
    return this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int, v5: Int, v6: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    setDim(4, v4)
    setDim(5, v5)
    setDim(6, v6)
    return this
  }


}

class CDIndex0D extends CDIndex {
  def this {
    this()
    super (0)
    this.size = 1
    this.offset = 0
  }

  def this(shape: Array[Int]) {
    this()
    super (shape)
  }

  def currentElement: Int = {
    return offset
  }

  def incr: Int = {
    return offset
  }

  def clone: Nothing = {
    return super.clone
  }

  def set: Nothing = {
    return this
  }
}

public

class CDIndex1D extends CDIndex {
  private var curr0: Int = 0
  private var stride0: Int = 0
  private var shape0: Int = 0

  def this {
    this()
    super (1)
  }

  def this(shape: Array[Int]) {
    this()
    super (shape)
    precalc
  }

  def toString: Nothing = {
    return Integer.toString(curr0)
  }

  protected def precalc {
    shape0 = shape(0)
    stride0 = stride(0)
    curr0 = current(0)
  }

  def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    return current.clone
  }

  def currentElement: Int = {
    return offset + curr0 * stride0
  }

  def incr: Int = {
    if (({
      curr0 += 1; curr0
    }) >= shape0) curr0 = 0
    return offset + curr0 * stride0
  }

  def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Nothing
    curr0 = value
  }

  def set0(v: Int): Nothing = {
    if (v < 0 || v >= shape0) throw new Nothing
    curr0 = v
    return this
  }

  def set(v0: Int): Nothing = {
    set0(v0)
    return this
  }

  def set(index: Array[Int]): Nothing = {
    if (index.length != rank) throw new Nothing
    set0(index(0))
    return this
  }

  def clone: Nothing = {
    return super.clone
  }

  private[ma2] def setDirect(v0: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Nothing
    return offset + v0 * stride0
  }
}

public

class CDIndex2D extends CDIndex {
  private var curr0: Int = 0
  ,
  private var curr1: Int = 0
  private var stride0: Int = 0
  ,
  private var stride1: Int = 0
  private var shape0: Int = 0
  ,
  private var shape1: Int = 0

  def this {
    this()
    super (2)
  }

  def this(shape: Array[Int]) {
    this()
    super (shape)
    precalc
  }

  protected def precalc {
    shape0 = shape(0)
    shape1 = shape(1)
    stride0 = stride(0)
    stride1 = stride(1)
    curr0 = current(0)
    curr1 = current(1)
  }

  def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current(1) = curr1
    return current.clone
  }

  def toString: Nothing = {
    return curr0 + "," + curr1
  }

  def currentElement: Int = {
    return offset + curr0 * stride0 + curr1 * stride1
  }

  def incr: Int = {
    if (({
      curr1 += 1; curr1
    }) >= shape1) {
      curr1 = 0
      if (({
        curr0 += 1; curr0
      }) >= shape0) {
        curr0 = 0
      }
    }
    return offset + curr0 * stride0 + curr1 * stride1
  }

  def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Nothing
    if (dim == 1) curr1 = value
    else curr0 = value
  }

  def set(index: Array[Int]): Nothing = {
    if (index.length != rank) throw new Nothing
    set0(index(0))
    set1(index(1))
    return this
  }

  def set0(v: Int): Nothing = {
    if (v < 0 || v >= shape0) throw new Nothing
    curr0 = v
    return this
  }

  def set1(v: Int): Nothing = {
    if (v < 0 || v >= shape1) throw new Nothing("index=" + v + " shape=" + shape1)
    curr1 = v
    return this
  }

  def set(v0: Int, v1: Int): Nothing = {
    set0(v0)
    set1(v1)
    return this
  }

  def clone: Nothing = {
    return super.clone
  }

  private[ma2] def setDirect(v0: Int, v1: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Nothing
    if (v1 < 0 || v1 >= shape1) throw new Nothing
    return offset + v0 * stride0 + v1 * stride1
  }
}

public

class CDIndex3D extends CDIndex {
  private var curr0: Int = 0
  ,
  private var curr1: Int = 0
  ,
  private var curr2: Int = 0
  private var stride0: Int = 0
  ,
  private var stride1: Int = 0
  ,
  private var stride2: Int = 0
  private var shape0: Int = 0
  ,
  private var shape1: Int = 0
  ,
  private var shape2: Int = 0

  def this {
    this()
    super (3)
  }

  def this(shape: Array[Int]) {
    this()
    super (shape)
    precalc
  }

  protected def precalc {
    shape0 = shape(0)
    shape1 = shape(1)
    shape2 = shape(2)
    stride0 = stride(0)
    stride1 = stride(1)
    stride2 = stride(2)
    curr0 = current(0)
    curr1 = current(1)
    curr2 = current(2)
  }

  def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current(1) = curr1
    current(2) = curr2
    return current.clone
  }

  def toString: Nothing = {
    return curr0 + "," + curr1 + "," + curr2
  }

  def currentElement: Int = {
    return offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2
  }

  def incr: Int = {
    if (({
      curr2 += 1; curr2
    }) >= shape2) {
      curr2 = 0
      if (({
        curr1 += 1; curr1
      }) >= shape1) {
        curr1 = 0
        if (({
          curr0 += 1; curr0
        }) >= shape0) {
          curr0 = 0
        }
      }
    }
    return offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2
  }

  def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Nothing
    if (dim == 2) curr2 = value
    else if (dim == 1) curr1 = value
    else curr0 = value
  }

  def set0(v: Int): Nothing = {
    if (v < 0 || v >= shape0) throw new Nothing
    curr0 = v
    return this
  }

  def set1(v: Int): Nothing = {
    if (v < 0 || v >= shape1) throw new Nothing
    curr1 = v
    return this
  }

  def set2(v: Int): Nothing = {
    if (v < 0 || v >= shape2) throw new Nothing
    curr2 = v
    return this
  }

  def set(v0: Int, v1: Int, v2: Int): Nothing = {
    set0(v0)
    set1(v1)
    set2(v2)
    return this
  }

  def set(index: Array[Int]): Nothing = {
    if (index.length != rank) throw new Nothing
    set0(index(0))
    set1(index(1))
    set2(index(2))
    return this
  }

  def clone: Nothing = {
    return super.clone
  }

  private[ma2] def setDirect(v0: Int, v1: Int, v2: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Nothing
    if (v1 < 0 || v1 >= shape1) throw new Nothing
    if (v2 < 0 || v2 >= shape2) throw new Nothing
    return offset + v0 * stride0 + v1 * stride1 + v2 * stride2
  }
}

public

class CDIndex4D extends CDIndex {
  private var curr0: Int = 0
  ,
  private var curr1: Int = 0
  ,
  private var curr2: Int = 0
  ,
  private var curr3: Int = 0
  private var stride0: Int = 0
  ,
  private var stride1: Int = 0
  ,
  private var stride2: Int = 0
  ,
  private var stride3: Int = 0
  private var shape0: Int = 0
  ,
  private var shape1: Int = 0
  ,
  private var shape2: Int = 0
  ,
  private var shape3: Int = 0

  def this {
    this()
    super (4)
  }

  def this(shape: Array[Int]) {
    this()
    super (shape)
    precalc
  }

  protected def precalc {
    shape0 = shape(0)
    shape1 = shape(1)
    shape2 = shape(2)
    shape3 = shape(3)
    stride0 = stride(0)
    stride1 = stride(1)
    stride2 = stride(2)
    stride3 = stride(3)
    curr0 = current(0)
    curr1 = current(1)
    curr2 = current(2)
    curr3 = current(3)
  }

  def toString: Nothing = {
    return curr0 + "," + curr1 + "," + curr2 + "," + curr3
  }

  def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current(1) = curr1
    current(2) = curr2
    current(3) = curr3
    return current.clone
  }

  def currentElement: Int = {
    return offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2 + +curr3 * stride3
  }

  def incr: Int = {
    if (({
      curr3 += 1;
      curr3
    }) >= shape3) {
      curr3 = 0
      if (({
        curr2 += 1;
        curr2
      }) >= shape2) {
        curr2 = 0
        if (({
          curr1 += 1;
          curr1
        }) >= shape1) {
          curr1 = 0
          if (({
            curr0 += 1;
            curr0
          }) >= shape0) {
            curr0 = 0
          }
        }
      }
    }
    return offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2 + curr3 * stride3
  }

  def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Nothing
    if (dim == 3) curr3 = value
    else if (dim == 2) curr2 = value
    else if (dim == 1) curr1 = value
    else curr0 = value
  }

  def set0(v: Int): Nothing = {
    if (v < 0 || v >= shape0) throw new Nothing
    curr0 = v
    return this
  }

  def set1(v: Int): Nothing = {
    if (v < 0 || v >= shape1) throw new Nothing
    curr1 = v
    return this
  }

  def set2(v: Int): Nothing = {
    if (v < 0 || v >= shape2) throw new Nothing
    curr2 = v
    return this
  }

  def set3(v: Int): Nothing = {
    if (v < 0 || v >= shape3) throw new Nothing
    curr3 = v
    return this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int): Nothing = {
    set0(v0)
    set1(v1)
    set2(v2)
    set3(v3)
    return this
  }

  def set(index: Array[Int]): Nothing = {
    if (index.length != rank) throw new Nothing
    set0(index(0))
    set1(index(1))
    set2(index(2))
    set3(index(3))
    return this
  }

  def clone: Nothing = {
    return super.clone
  }

  private[ma2] def setDirect(v0: Int, v1: Int, v2: Int, v3: Int): Int = {
    return offset + v0 * stride0 + v1 * stride1 + v2 * stride2 + v3 * stride3
  }


}

public

class CDIndex5D extends CDIndex {
  private var curr0: Int = 0
  ,
  private var curr1: Int = 0
  ,
  private var curr2: Int = 0
  ,
  private var curr3: Int = 0
  ,
  private var curr4: Int = 0
  private var stride0: Int = 0
  ,
  private var stride1: Int = 0
  ,
  private var stride2: Int = 0
  ,
  private var stride3: Int = 0
  ,
  private var stride4: Int = 0
  private var shape0: Int = 0
  ,
  private var shape1: Int = 0
  ,
  private var shape2: Int = 0
  ,
  private var shape3: Int = 0
  ,
  private var shape4: Int = 0

  def this {
    this()
    super (5)
  }

  def this(shape: Array[Int]) {
    this()
    super (shape)
    precalc
  }

  protected def precalc {
    shape0 = shape(0)
    shape1 = shape(1)
    shape2 = shape(2)
    shape3 = shape(3)
    shape4 = shape(4)
    stride0 = stride(0)
    stride1 = stride(1)
    stride2 = stride(2)
    stride3 = stride(3)
    stride4 = stride(4)
    curr0 = current(0)
    curr1 = current(1)
    curr2 = current(2)
    curr3 = current(3)
    curr4 = current(4)
  }

  def toString: Nothing = {
    return curr0 + "," + curr1 + "," + curr2 + "," + curr3 + "," + curr4
  }

  def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current(1) = curr1
    current(2) = curr2
    current(3) = curr3
    current(4) = curr4
    return current.clone
  }

  def currentElement: Int = {
    return offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2 + +curr3 * stride3 + curr4 * stride4
  }

  def incr: Int = {
    if (({
      curr4 += 1; curr4
    }) >= shape4) {
      curr4 = 0
      if (({
        curr3 += 1; curr3
      }) >= shape3) {
        curr3 = 0
        if (({
          curr2 += 1; curr2
        }) >= shape2) {
          curr2 = 0
          if (({
            curr1 += 1; curr1
          }) >= shape1) {
            curr1 = 0
            if (({
              curr0 += 1; curr0
            }) >= shape0) {
              curr0 = 0
            }
          }
        }
      }
    }
    return offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2 + +curr3 * stride3 + curr4 * stride4
  }

  def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Nothing
    if (dim == 4) curr4 = value
    else if (dim == 3) curr3 = value
    else if (dim == 2) curr2 = value
    else if (dim == 1) curr1 = value
    else curr0 = value
  }

  def set0(v: Int): Nothing = {
    if (v < 0 || v >= shape0) throw new Nothing
    curr0 = v
    return this
  }

  def set1(v: Int): Nothing = {
    if (v < 0 || v >= shape1) throw new Nothing
    curr1 = v
    return this
  }

  def set2(v: Int): Nothing = {
    if (v < 0 || v >= shape2) throw new Nothing
    curr2 = v
    return this
  }

  def set3(v: Int): Nothing = {
    if (v < 0 || v >= shape3) throw new Nothing
    curr3 = v
    return this
  }

  def set4(v: Int): Nothing = {
    if (v < 0 || v >= shape4) throw new Nothing
    curr4 = v
    return this
  }

  def set(index: Array[Int]): Nothing = {
    if (index.length != rank) throw new Nothing
    set0(index(0))
    set1(index(1))
    set2(index(2))
    set3(index(3))
    set4(index(4))
    return this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int): Nothing = {
    set0(v0)
    set1(v1)
    set2(v2)
    set3(v3)
    set4(v4)
    return this
  }

  private[ma2] def setDirect(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int): Int = {
    return offset + v0 * stride0 + v1 * stride1 + v2 * stride2 + v3 * stride3 + v4 * stride4
  }
}













