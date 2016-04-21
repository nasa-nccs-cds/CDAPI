package nasa.nccs.cdapi.tensors
import scala.collection.mutable.ListBuffer
import ucar.ma2
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
// Based on ucar.ma2.CDIndex, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

object CDIndex {

  def factory( index: CDIndex ): CDIndex = factory( index.shape, index.stride, index.offset )

  def factory( shape: Array[Int], stride: Array[Int]=Array.emptyIntArray, offset: Int = 0 ): CDIndex = shape.length match {
    case 1 =>
      return new CDIndex1D(shape,stride,offset)
    case 2 =>
      return new CDIndex2D(shape,stride,offset)
    case 3 =>
      return new CDIndex3D(shape,stride,offset)
    case 4 =>
      return new CDIndex4D(shape,stride,offset)
    case 5 =>
      return new CDIndex5D(shape,stride,offset)
    case _ =>
      return new CDIndex(shape,stride,offset)
  }

  def computeSize(shape: Array[Int]): Long = { // shape.filter( i => i >= 0 )
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


class CDRawDataFloatIterator( protected val _maa: CDArrayFloat  ) extends collection.Iterator[Float] {
  private var currElement: Int = 0
  private val size = _maa.getSize

  def hasNext: Boolean = ( currElement < size - 1 )
  def next(): Float = {
    currElement += 1
    _maa.getFloat( currElement )
  }
  def getIndex() =  currElement
}

class CDIterator( index: CDIndex  ) extends collection.Iterator[CDIndex] {
  protected val counter: CDIndex = CDIndex.factory( index )
  private var count: Int = 0
  private var currElement: Int = 0

  def hasNext: Boolean = ( count < counter.size )
  def next(): CDIndex = {
    count += 1
    currElement = counter.incr
    counter
  }
  def getIndices: Array[Int] = counter.getCurrentCounter
  def getIndex() =  currElement
}

class CDIndex( val shape: Array[Int], _stride: Array[Int]=Array.emptyIntArray, val offset: Int = 0 ) {
  val rank: Int = shape.length
  val size: Long = CDIndex.computeSize(shape)
  val stride = if( _stride.isEmpty ) CDIndex.computeStrides(shape) else _stride
  val hasvlen: Boolean = (shape.length > 0 && shape(shape.length - 1) < 0)
  protected var current: Array[Int] = new Array[Int](rank)

  def this( index: CDIndex ) = {
    this( index.shape, index.stride, index.offset )
  }

  protected def precalc() {}

  def initIteration(): CDIndex = {
    assert (rank > 0, "Can't itereate a 0-rank array")
    current(rank - 1) = -1
    precalc()
    this
  }

  def flip(index: Int): CDIndex = {
    if ((index < 0) || (index >= rank)) throw new Exception()
    val i: CDIndex = this.clone.asInstanceOf[CDIndex]
    if (shape(index) >= 0) {
      i.offset += stride(index) * (shape(index) - 1)
      i.stride(index) = -stride(index)
    }
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

    var _offset: Int = offset
    val _shape: Array[Int] = Array.fill[Int](rank)(0)
    val _stride: Array[Int] = Array.fill[Int](rank)(0)
    for( ii <-(0 until rank); r = ranges(ii) ) {
      if (r == null) {
        _shape(ii) = shape(ii)
        _stride(ii) = stride(ii)
      }
      else {
        _shape(ii) = r.length
        _stride(ii) = stride(ii) * r.stride
        _offset += stride(ii) * r.first
      }
    }
    CDIndex.factory( _shape, _stride, _offset )
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
    val _shape = ListBuffer[Int]()
    val _stride = ListBuffer[Int]()
    for( ii <-(0 until rank); if (ii != dim) ) {
        _shape.append( shape(ii) )
        _stride.append( stride(ii) )
    }
    CDIndex.factory( _shape.toArray, _stride.toArray )
  }

  def transpose(index1: Int, index2: Int): CDIndex = {
    assert((index1 >= 0) && (index1 < rank), "illegal index in transpose " + index1 )
    assert((index2 >= 0) && (index2 < rank), "illegal index in transpose " + index1 )
    val _shape = shape.clone()
    val _stride = stride.clone()
    _stride(index1) = stride(index2)
    _stride(index2) = stride(index1)
    _shape(index1) = shape(index2)
    _shape(index2) = shape(index1)
    CDIndex.factory( _shape, _stride )
  }

  def permute(dims: Array[Int]): CDIndex = {
    assert( (dims.length == shape.length), "illegal shape in permute " + dims )
    for (dim <- dims) if ((dim < 0) || (dim >= rank)) throw new Exception( "illegal shape in permute " + dims )
    val _shape = ListBuffer[Int]()
    val _stride = ListBuffer[Int]()
    for( i <-(0 until dims.length) ) {
      _stride.append( stride(dims(i) ) )
      _shape.append( shape(dims(i)) )
    }
    CDIndex.factory( _shape.toArray, _stride.toArray )
  }

  def getRank: Int = {
    return rank
  }

  def getShape: Array[Int] = shape

  def getShape(index: Int): Int = {
    return shape(index)
  }

  def getSize: Long = {
    return size
  }

  def currentElement: Int = {
    var value: Int = offset
    var ii: Int = 0
    for( ii <-(0 until rank); if (shape(ii) >= 0) ) {
      value += current(ii) * stride(ii)
    }
    value
  }

  def getCurrentCounter: Array[Int] = current.clone

  def setCurrentCounter( _currElement: Int ) {
    var currElement = _currElement
    currElement -= offset
    for( ii <-(0 until rank) ) if (shape(ii) < 0) { current(ii) = -1 } else {
      current(ii) = currElement / stride(ii)
      currElement -= current(ii) * stride(ii)
    }
    set(current)
  }

  def incr: Int = {
    for( digit <-(rank - 1 to 0 by -1) ) if (shape(digit) < 0) { current(digit) = -1 } else {
      current(digit) += 1
      if (current(digit) < shape(digit)) return currentElement
      current(digit) = 0
    }
    currentElement
  }

  def set(index: Array[Int]): CDIndex = {
    assert( (index.length == rank), "Array has wrong rank in Index.set" )
    if (rank > 0) {
      val prefixrank: Int = (if (hasvlen) rank else rank - 1)
      Array.copy(index, 0, current, 0, prefixrank)
      if (hasvlen) current(prefixrank) = -1
    }
    this
  }

  def setDim(dim: Int, value: Int) {
    assert (value >= 0 && value < shape(dim), "Illegal argument in Index.setDim")
    if (shape(dim) >= 0) current(dim) = value
  }

  def set0(v: Int): CDIndex = {
    setDim(0, v)
    this
  }

  def set1(v: Int): CDIndex = {
    setDim(1, v)
    this
  }

  def set2(v: Int): CDIndex = {
    setDim(2, v)
    this
  }

  def set3(v: Int): CDIndex = {
    setDim(3, v)
    this
  }

  def set4(v: Int): CDIndex = {
    setDim(4, v)
    this
  }

  def set5(v: Int): CDIndex = {
    setDim(5, v)
    this
  }

  def set6(v: Int): CDIndex = {
    setDim(6, v)
    this
  }

  def set(v0: Int): CDIndex = {
    setDim(0, v0)
    this
  }

  def set(v0: Int, v1: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    this
  }

  def set(v0: Int, v1: Int, v2: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    setDim(4, v4)
    this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int, v5: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    setDim(4, v4)
    setDim(5, v5)
    this
  }

  def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int, v5: Int, v6: Int): CDIndex = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    setDim(4, v4)
    setDim(5, v5)
    setDim(6, v6)
    this
  }


}


class CDIndex1D(shape: Array[Int], stride: Array[Int], offset: Int = 0) extends CDIndex(shape,stride,offset) {
  private var curr0: Int = 0
  private var stride0: Int = 0
  private var shape0: Int = 0


  def this(shape: Array[Int]) {
    this(shape)
    precalc
  }


  override def precalc {
    shape0 = shape(0)
    stride0 = stride(0)
    curr0 = current(0)
  }

  override def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current.clone
  }

  override def currentElement: Int = {
    offset + curr0 * stride0
  }

  override def incr: Int = {
    if (({
      curr0 += 1; curr0
    }) >= shape0) curr0 = 0
    offset + curr0 * stride0
  }

  override def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Exception()
    curr0 = value
  }

  override def set0(v: Int): CDIndex = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set(v0: Int): CDIndex = {
    set0(v0)
    this
  }

  override def set(index: Array[Int]): CDIndex = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    this
  }
  
  private def setDirect(v0: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Exception()
    offset + v0 * stride0
  }
}

class CDIndex2D(shape: Array[Int], stride: Array[Int], offset: Int = 0) extends CDIndex(shape,stride,offset) {
  private var curr0: Int = 0
  private var curr1: Int = 0
  private var stride0: Int = 0
  private var stride1: Int = 0
  private var shape0: Int = 0
  private var shape1: Int = 0


  def this(shape: Array[Int]) {
    this (shape)
    precalc
  }

  override def precalc {
    shape0 = shape(0)
    shape1 = shape(1)
    stride0 = stride(0)
    stride1 = stride(1)
    curr0 = current(0)
    curr1 = current(1)
  }

  override def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current(1) = curr1
    current.clone
  }

  override def toString: String = {
    curr0 + "," + curr1
  }

  override def currentElement: Int = {
    offset + curr0 * stride0 + curr1 * stride1
  }

  override def incr: Int = {
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
    offset + curr0 * stride0 + curr1 * stride1
  }

  override def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Exception()
    if (dim == 1) curr1 = value
    else curr0 = value
  }

  override def set(index: Array[Int]): CDIndex = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    set1(index(1))
    this
  }

  override def set0(v: Int): CDIndex = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set1(v: Int): CDIndex = {
    if (v < 0 || v >= shape1) throw new Exception()("index=" + v + " shape=" + shape1)
    curr1 = v
    this
  }

  override def set(v0: Int, v1: Int): CDIndex = {
    set0(v0)
    set1(v1)
    this
  }


  private[ma2] def setDirect(v0: Int, v1: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Exception()
    if (v1 < 0 || v1 >= shape1) throw new Exception()
    offset + v0 * stride0 + v1 * stride1
  }
}


class CDIndex3D(shape: Array[Int], stride: Array[Int], offset: Int = 0) extends CDIndex(shape,stride,offset) {
  private var curr0: Int = 0
  private var curr1: Int = 0

  private var curr2: Int = 0
  private var stride0: Int = 0

  private var stride1: Int = 0

  private var stride2: Int = 0
  private var shape0: Int = 0

  private var shape1: Int = 0

  private var shape2: Int = 0


  def this(shape: Array[Int]) {
    this (shape)
    precalc
  }

  override  def precalc {
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

  override def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current(1) = curr1
    current(2) = curr2
    current.clone
  }

  override def toString: String = {
    curr0 + "," + curr1 + "," + curr2
  }

  override def currentElement: Int = {
    offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2
  }

  override def incr: Int = {
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
    offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2
  }

  override def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Exception()
    if (dim == 2) curr2 = value
    else if (dim == 1) curr1 = value
    else curr0 = value
  }

  override def set0(v: Int): CDIndex = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set1(v: Int): CDIndex = {
    if (v < 0 || v >= shape1) throw new Exception()
    curr1 = v
    this
  }

  override def set2(v: Int): CDIndex = {
    if (v < 0 || v >= shape2) throw new Exception()
    curr2 = v
    this
  }

  override def set(v0: Int, v1: Int, v2: Int): CDIndex = {
    set0(v0)
    set1(v1)
    set2(v2)
    this
  }

  override def set(index: Array[Int]): CDIndex = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    set1(index(1))
    set2(index(2))
    this
  }

  private[ma2] def setDirect(v0: Int, v1: Int, v2: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Exception()
    if (v1 < 0 || v1 >= shape1) throw new Exception()
    if (v2 < 0 || v2 >= shape2) throw new Exception()
    offset + v0 * stride0 + v1 * stride1 + v2 * stride2
  }
}

class CDIndex4D(shape: Array[Int], stride: Array[Int], offset: Int = 0) extends CDIndex(shape,stride,offset) {
  private var curr0: Int = 0

  private var curr1: Int = 0

  private var curr2: Int = 0

  private var curr3: Int = 0
  private var stride0: Int = 0

  private var stride1: Int = 0

  private var stride2: Int = 0

  private var stride3: Int = 0
  private var shape0: Int = 0

  private var shape1: Int = 0

  private var shape2: Int = 0

  private var shape3: Int = 0


  def this(shape: Array[Int]) {
    this (shape)
    precalc
  }

  override def precalc {
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

  override def toString: String = {
    curr0 + "," + curr1 + "," + curr2 + "," + curr3
  }

  override def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current(1) = curr1
    current(2) = curr2
    current(3) = curr3
    current.clone
  }

  override def currentElement: Int = {
    offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2 + +curr3 * stride3
  }

  override def incr: Int = {
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
    offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2 + curr3 * stride3
  }

  override def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Exception()
    if (dim == 3) curr3 = value
    else if (dim == 2) curr2 = value
    else if (dim == 1) curr1 = value
    else curr0 = value
  }

  override def set0(v: Int): CDIndex = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set1(v: Int): CDIndex = {
    if (v < 0 || v >= shape1) throw new Exception()
    curr1 = v
    this
  }

  override def set2(v: Int): CDIndex = {
    if (v < 0 || v >= shape2) throw new Exception()
    curr2 = v
    this
  }

  override def set3(v: Int): CDIndex = {
    if (v < 0 || v >= shape3) throw new Exception()
    curr3 = v
    this
  }

  override def set(v0: Int, v1: Int, v2: Int, v3: Int): CDIndex = {
    set0(v0)
    set1(v1)
    set2(v2)
    set3(v3)
    this
  }

  override def set(index: Array[Int]): CDIndex = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    set1(index(1))
    set2(index(2))
    set3(index(3))
    this
  }

  private def setDirect(v0: Int, v1: Int, v2: Int, v3: Int): Int = {
    offset + v0 * stride0 + v1 * stride1 + v2 * stride2 + v3 * stride3
  }
}


class CDIndex5D(shape: Array[Int], stride: Array[Int], offset: Int = 0) extends CDIndex(shape,stride,offset) {
  private var curr0: Int = 0

  private var curr1: Int = 0

  private var curr2: Int = 0

  private var curr3: Int = 0

  private var curr4: Int = 0
  private var stride0: Int = 0

  private var stride1: Int = 0

  private var stride2: Int = 0

  private var stride3: Int = 0

  private var stride4: Int = 0
  private var shape0: Int = 0

  private var shape1: Int = 0

  private var shape2: Int = 0

  private var shape3: Int = 0

  private var shape4: Int = 0


  def this(shape: Array[Int]) {
    this (shape)
    precalc
  }

  override  def precalc {
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

  override def toString: String = {
    curr0 + "," + curr1 + "," + curr2 + "," + curr3 + "," + curr4
  }

  override def getCurrentCounter: Array[Int] = {
    current(0) = curr0
    current(1) = curr1
    current(2) = curr2
    current(3) = curr3
    current(4) = curr4
    current.clone
  }

  override def currentElement: Int = {
    offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2 + +curr3 * stride3 + curr4 * stride4
  }

  override def incr: Int = {
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
    offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2 + +curr3 * stride3 + curr4 * stride4
  }

  override def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Exception()
    if (dim == 4) curr4 = value
    else if (dim == 3) curr3 = value
    else if (dim == 2) curr2 = value
    else if (dim == 1) curr1 = value
    else curr0 = value
  }

  override def set0(v: Int): CDIndex = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set1(v: Int): CDIndex = {
    if (v < 0 || v >= shape1) throw new Exception()
    curr1 = v
    this
  }

  override def set2(v: Int): CDIndex = {
    if (v < 0 || v >= shape2) throw new Exception()
    curr2 = v
    this
  }

  override def set3(v: Int): CDIndex = {
    if (v < 0 || v >= shape3) throw new Exception()
    curr3 = v
    this
  }

  override def set4(v: Int): CDIndex = {
    if (v < 0 || v >= shape4) throw new Exception()
    curr4 = v
    this
  }

  override def set(index: Array[Int]): CDIndex = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    set1(index(1))
    set2(index(2))
    set3(index(3))
    set4(index(4))
    this
  }

  override def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int): CDIndex = {
    set0(v0)
    set1(v1)
    set2(v2)
    set3(v3)
    set4(v4)
    this
  }

  private def setDirect(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int): Int = {
    offset + v0 * stride0 + v1 * stride1 + v2 * stride2 + v3 * stride3 + v4 * stride4
  }
}













