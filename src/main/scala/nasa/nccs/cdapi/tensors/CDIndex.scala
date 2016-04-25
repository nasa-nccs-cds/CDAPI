package nasa.nccs.cdapi.tensors
import sun.font.TrueTypeFont

import scala.collection.mutable.ListBuffer
import ucar.ma2

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
// Based on ucar.ma2.CDIndex, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

object CDIterator {

  def factory( index: CDIndex ): CDArrayIndexIterator = index.getRank match {
    case 1 =>
      return new CDIndexIterator1D(index)
    case 2 =>
      return new CDIndexIterator2D(index)
    case 3 =>
      return new CDIndexIterator3D(index)
    case 4 =>
      return new CDIndexIterator4D(index)
    case 5 =>
      return new CDIndexIterator5D(index)
    case _ =>
      return new CDArrayIndexIterator(index)
  }

}

abstract class CDIterator( index: CDIndex  ) extends collection.Iterator[Int] {
  protected val cdIndex: CDIndex = CDIndex.factory( index )
  protected val rank = cdIndex.getRank
  protected val stride = cdIndex.getStride
  protected val shape = cdIndex.getShape
  protected val offset = cdIndex.getOffset
  protected var coordIndices: Array[Int] = new Array[Int]( cdIndex.getRank )
  protected val hasvlen: Boolean = (shape.length > 0 && shape(shape.length - 1) < 0)

  def hasNext: Boolean
  def next(): Int
  def getCoordinateIndices: Array[Int]
  def getIndex: Int
  def initialize: Unit

  protected def currentElement: Int = {
    var value: Int = offset
    for( ii <-(0 until rank ); if (shape(ii) >= 0) ) {
      value += coordIndices(ii) * stride(ii)
    }
    value
  }

  protected def getCoordIndices: Array[Int] = coordIndices.clone

  protected def setCurrentCounter( _currElement: Int ) {
    var currElement = _currElement
    currElement -= offset
    for( ii <-(0 until rank ) ) if (shape(ii) < 0) { coordIndices(ii) = -1 } else {
      coordIndices(ii) = currElement / stride(ii)
      currElement -= coordIndices(ii) * stride(ii)
    }
  }

  protected def incr: Int = {
    for( digit <-(rank  - 1 to 0 by -1) ) if (shape(digit) < 0) { coordIndices(digit) = -1 } else {
      coordIndices(digit) += 1
      if (coordIndices(digit) < shape(digit)) return currentElement
      coordIndices(digit) = 0
    }
    currentElement
  }

  protected def setCoordIndices(newCoordIndices: Array[Int]): CDIterator = {
    assert( (newCoordIndices.length == rank ), "Array has wrong rank in Index.set" )
    if (rank  > 0) {
      val prefixrank: Int = (if (hasvlen) rank else rank - 1)
      Array.copy(newCoordIndices, 0, coordIndices, 0, prefixrank)
      if (hasvlen) coordIndices(prefixrank) = -1
    }
    this
  }

  protected def setDim(dim: Int, value: Int) {
    assert (value >= 0 && value < shape(dim), "Illegal argument in Index.setDim")
    if (shape(dim) >= 0) coordIndices(dim) = value
  }

  protected def set0(v: Int): CDIterator = {
    setDim(0, v)
    this
  }

  protected def set1(v: Int): CDIterator = {
    setDim(1, v)
    this
  }

  protected def set2(v: Int): CDIterator = {
    setDim(2, v)
    this
  }

  protected def set3(v: Int): CDIterator = {
    setDim(3, v)
    this
  }

  protected def set4(v: Int): CDIterator = {
    setDim(4, v)
    this
  }

  protected def set5(v: Int): CDIterator = {
    setDim(5, v)
    this
  }

  protected def set(v0: Int): CDIterator = {
    setDim(0, v0)
    this
  }

  protected def set(v0: Int, v1: Int): CDIterator = {
    setDim(0, v0)
    setDim(1, v1)
    this
  }

  protected def set(v0: Int, v1: Int, v2: Int): CDIterator = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    this
  }

  protected def set(v0: Int, v1: Int, v2: Int, v3: Int): CDIterator = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    this
  }

  protected def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int): CDIterator = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    setDim(4, v4)
    this
  }

  protected def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int, v5: Int): CDIterator = {
    setDim(0, v0)
    setDim(1, v1)
    setDim(2, v2)
    setDim(3, v3)
    setDim(4, v4)
    setDim(5, v5)
    this
  }
}

class CDArrayIndexIterator( index: CDIndex  ) extends CDIterator(index) {
  private var count: Int = 0
  private var currElement: Int = currentElement
  private var numElem = cdIndex.getSize

  def hasNext: Boolean = ( count < numElem )
  def next(): Int = {
    if(count>0) { currElement = incr }
    count += 1
    currElement
  }
  def getCoordinateIndices: Array[Int] = getCoordIndices
  def getIndex: Int =  currElement
  def initialize: Unit = {}
}

class CDStorageIndexIterator( index: CDIndex  ) extends CDIterator(index) {
  private var count: Int = -1
  private var countBound = cdIndex.getSize - 1

  def hasNext: Boolean = ( count < countBound )
  def next(): Int = {
    count += 1
    count
  }
  def getCoordinateIndices: Array[Int] = { setCurrentCounter( count ); getCoordIndices }
  def getIndex: Int =  count
  def initialize: Unit = {}
}

object CDIndex {

  def factory(index: CDIndex): CDIndex = new CDIndex(index.getShape, index.getStride, index.getOffset )
  def factory(shape: Array[Int], _stride: Array[Int]=Array.emptyIntArray, offset: Int = 0): CDIndex = new CDIndex(shape, _stride, offset )
}

class CDIndex( protected val shape: Array[Int], _stride: Array[Int]=Array.emptyIntArray, protected val offset: Int = 0 ) {
  protected val rank: Int = shape.length
  protected val stride = if( _stride.isEmpty ) computeStrides(shape) else _stride

  def this( index: CDIndex ) = {
    this( index.shape, index.stride, index.offset )
  }

  override def toString: String = {
    "{ Shape: " + shape.mkString("[ ",", "," ], Stride: " + _stride.mkString("[ ",", "," ]") + " Offset: " + offset + " } ")
  }

  def broadcasted: Boolean = {
    for( i <- (0 until rank) ) if( (stride(i) == 0) && (shape(i) > 1) ) return true
    false
  }

  def computeStrides( shape: Array[Int] ): Array[Int] = {
    var product: Int = 1
    var strides = for (ii <- (shape.length - 1 to 0 by -1); thisDim = shape(ii) ) yield
      if (thisDim >= 0) {
        val curr_stride = product
        product *= thisDim
        curr_stride
      } else { 0 }
    return strides.reverse.toArray
  }

  def flip(index: Int): CDIndex = {
    assert ( (index >= 0) && (index < rank), "Illegal rank index: " +  index )
    val new_index = if (shape(index) >= 0) {
      val _offset = offset + stride(index) * (shape(index) - 1)
      val _stride = stride.clone
      _stride(index) = -stride(index)
      new CDIndex( shape, _stride, _offset )
    } else new CDIndex( this )
    return new_index
  }

  def section( ranges: List[ma2.Range] ): CDIndex = {
    assert(ranges.size == rank, "Bad ranges [] length")
    for( ii <-(0 until rank); r = ranges(ii); if ((r != null) && (r != ma2.Range.VLEN)) ) {
      assert ((r.first >= 0) && (r.first < shape(ii)), "Bad range starting value at index " + ii + " == " + r.first)
      assert ((r.last >= 0) && (r.last < shape(ii)), "Bad range ending value at index " + ii + " == " + r.last)
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
    CDIndex.factory( _shape.toArray, _stride.toArray, offset )
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
    CDIndex.factory( _shape, _stride, offset )
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
    CDIndex.factory( _shape.toArray, _stride.toArray, offset )
  }

  def broadcast(  dim: Int, size: Int ): CDIndex = {
    assert( shape(dim) == 1, "Can't broadcast a dimension with size > 1" )
    val _shape = shape.clone()
    val _stride = stride.clone()
    _shape(dim) = size
    _stride(dim) = 0
    CDIndex.factory( _shape, _stride, offset )
  }

  def broadcast( bcast_shape: Array[Int] ): CDIndex = {
    assert ( bcast_shape.length == rank, "Can't broadcast shape (%s) to (%s)".format( shape.mkString(","), bcast_shape.mkString(",") ) )
    val _shape = shape.clone()
    val _stride = stride.clone()
    for( idim <- (0 until rank ); bsize = bcast_shape(idim); size0 = shape(idim); if( bsize != size0 )  ) {
      assert((size0 == 1) || (bsize == size0), "Can't broadcast shape (%s) to (%s)".format(shape.mkString(","), bcast_shape.mkString(",")))
      _shape(idim) = bsize
      _stride(idim) = 0
    }
    CDIndex.factory( _shape, _stride, offset )
  }

  def getRank: Int = {
    return rank
  }

  def getShape: Array[Int] = shape.clone
  def getStride: Array[Int] = stride.clone

  def getShape(index: Int): Int = {
    return shape(index)
  }

  def getSize: Long = shape.filter( _ > 0 ).product

  def getOffset: Int = offset

}


class CDIndexIterator1D( index: CDIndex ) extends  CDArrayIndexIterator( index  ) {
  private var curr0: Int = coordIndices(0)
  private var stride0: Int = stride(0)
  private var shape0: Int = shape(0)

  override def initialize {
    shape0 = shape(0)
    stride0 = stride(0)
    curr0 = coordIndices(0)
  }

  override def getCoordIndices: Array[Int] = {
    coordIndices(0) = curr0
    coordIndices.clone
  }

  override def currentElement: Int = {
    offset + curr0 * stride0
  }

  override def incr: Int = {
    if (({ curr0 += 1; curr0 }) >= shape0) curr0 = 0
    offset + curr0 * stride0
  }

  override def setDim(dim: Int, value: Int) {
    if (value < 0 || value >= shape(dim)) throw new Exception()
    curr0 = value
  }

  override def set0(v: Int): CDIndexIterator1D = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set(v0: Int): CDIndexIterator1D = {
    set0(v0)
    this
  }

  override def setCoordIndices(index: Array[Int]): CDIndexIterator1D = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    this
  }
  
  private def setDirect(v0: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Exception()
    offset + v0 * stride0
  }
}

class CDIndexIterator2D( index: CDIndex ) extends  CDArrayIndexIterator( index  ) {
  private var curr0: Int = coordIndices(0)
  private var curr1: Int = coordIndices(1)
  private var stride0: Int = stride(0)
  private var stride1: Int = stride(1)
  private var shape0: Int = shape(0)
  private var shape1: Int = shape(1)


  override def initialize {
    shape0 = shape(0)
    shape1 = shape(1)
    stride0 = stride(0)
    stride1 = stride(1)
    curr0 = coordIndices(0)
    curr1 = coordIndices(1)
  }

  override def getCoordIndices: Array[Int] = {
    coordIndices(0) = curr0
    coordIndices(1) = curr1
    coordIndices.clone
  }

  override def toString: String = {
    curr0 + "," + curr1
  }

  override def currentElement: Int = {
    offset + curr0 * stride0 + curr1 * stride1
  }

  override def incr: Int = {
    if (({ curr1 += 1; curr1 }) >= shape1) {
      curr1 = 0
      if (({ curr0 += 1; curr0 }) >= shape0) {
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

  override def setCoordIndices(index: Array[Int]): CDIndexIterator2D = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    set1(index(1))
    this
  }

  override def set0(v: Int): CDIndexIterator2D = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set1(v: Int): CDIndexIterator2D = {
    if (v < 0 || v >= shape1) throw new Exception("index=" + v + " shape=" + shape1)
    curr1 = v
    this
  }

  override def set(v0: Int, v1: Int): CDIndexIterator2D = {
    set0(v0)
    set1(v1)
    this
  }


  private def setDirect(v0: Int, v1: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Exception()
    if (v1 < 0 || v1 >= shape1) throw new Exception()
    offset + v0 * stride0 + v1 * stride1
  }
}


class CDIndexIterator3D( index: CDIndex ) extends  CDArrayIndexIterator( index  ) {
  private var curr0: Int = coordIndices(0)
  private var curr1: Int = coordIndices(1)
  private var curr2: Int = coordIndices(2)
  private var stride0: Int = stride(0)
  private var stride1: Int = stride(1)
  private var stride2: Int = stride(2)
  private var shape0: Int = shape(0)
  private var shape1: Int = shape(1)
  private var shape2: Int = shape(2)


  override  def initialize {
    shape0 = shape(0)
    shape1 = shape(1)
    shape2 = shape(2)
    stride0 = stride(0)
    stride1 = stride(1)
    stride2 = stride(2)
    curr0 = coordIndices(0)
    curr1 = coordIndices(1)
    curr2 = coordIndices(2)
  }

  override def getCoordIndices: Array[Int] = {
    coordIndices(0) = curr0
    coordIndices(1) = curr1
    coordIndices(2) = curr2
    coordIndices.clone
  }

  override def toString: String = {
    curr0 + "," + curr1 + "," + curr2
  }

  override def currentElement: Int = {
    offset + curr0 * stride0 + curr1 * stride1 + curr2 * stride2
  }

  override def incr: Int = {
    if (({ curr2 += 1; curr2 }) >= shape2) {
      curr2 = 0
      if (({ curr1 += 1; curr1 }) >= shape1) {
        curr1 = 0
        if (({ curr0 += 1; curr0 }) >= shape0) {
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

  override def set0(v: Int): CDIndexIterator3D = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set1(v: Int): CDIndexIterator3D = {
    if (v < 0 || v >= shape1) throw new Exception()
    curr1 = v
    this
  }

  override def set2(v: Int): CDIndexIterator3D = {
    if (v < 0 || v >= shape2) throw new Exception()
    curr2 = v
    this
  }

  override def set(v0: Int, v1: Int, v2: Int): CDIndexIterator3D = {
    set0(v0)
    set1(v1)
    set2(v2)
    this
  }

  override def setCoordIndices(index: Array[Int]): CDIndexIterator3D = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    set1(index(1))
    set2(index(2))
    this
  }

  private def setDirect(v0: Int, v1: Int, v2: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Exception()
    if (v1 < 0 || v1 >= shape1) throw new Exception()
    if (v2 < 0 || v2 >= shape2) throw new Exception()
    offset + v0 * stride0 + v1 * stride1 + v2 * stride2
  }
}

class CDIndexIterator4D( index: CDIndex ) extends  CDArrayIndexIterator( index  ) {
  private var curr0: Int = coordIndices(0)
  private var curr1: Int = coordIndices(1)
  private var curr2: Int = coordIndices(2)
  private var curr3: Int = coordIndices(3)
  private var stride0: Int = stride(0)
  private var stride1: Int = stride(1)
  private var stride2: Int = stride(2)
  private var stride3: Int = stride(3)
  private var shape0: Int = shape(0)
  private var shape1: Int = shape(1)
  private var shape2: Int = shape(2)
  private var shape3: Int = shape(3)

  override def initialize {
    shape0 = shape(0)
    shape1 = shape(1)
    shape2 = shape(2)
    shape3 = shape(3)
    stride0 = stride(0)
    stride1 = stride(1)
    stride2 = stride(2)
    stride3 = stride(3)
    curr0 = coordIndices(0)
    curr1 = coordIndices(1)
    curr2 = coordIndices(2)
    curr3 = coordIndices(3)
  }

  override def toString: String = {
    curr0 + "," + curr1 + "," + curr2 + "," + curr3
  }

  override def getCoordIndices: Array[Int] = {
    coordIndices(0) = curr0
    coordIndices(1) = curr1
    coordIndices(2) = curr2
    coordIndices(3) = curr3
    coordIndices.clone
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

  override def set0(v: Int): CDIndexIterator4D = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set1(v: Int): CDIndexIterator4D = {
    if (v < 0 || v >= shape1) throw new Exception()
    curr1 = v
    this
  }

  override def set2(v: Int): CDIndexIterator4D = {
    if (v < 0 || v >= shape2) throw new Exception()
    curr2 = v
    this
  }

  override def set3(v: Int): CDIndexIterator4D = {
    if (v < 0 || v >= shape3) throw new Exception()
    curr3 = v
    this
  }

  override def set(v0: Int, v1: Int, v2: Int, v3: Int): CDIndexIterator4D = {
    set0(v0)
    set1(v1)
    set2(v2)
    set3(v3)
    this
  }

  override def setCoordIndices(index: Array[Int]): CDIndexIterator4D = {
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


class CDIndexIterator5D( index: CDIndex ) extends  CDArrayIndexIterator( index  ) {
  private var curr0: Int = coordIndices(0)
  private var curr1: Int = coordIndices(1)
  private var curr2: Int = coordIndices(2)
  private var curr3: Int = coordIndices(3)
  private var curr4: Int = coordIndices(4)
  private var stride0: Int = stride(0)
  private var stride1: Int = stride(1)
  private var stride2: Int = stride(2)
  private var stride3: Int = stride(3)
  private var stride4: Int = stride(4)
  private var shape0: Int = shape(0)
  private var shape1: Int = shape(1)
  private var shape2: Int = shape(2)
  private var shape3: Int = shape(3)
  private var shape4: Int = shape(4)


  override  def initialize {
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
    curr0 = coordIndices(0)
    curr1 = coordIndices(1)
    curr2 = coordIndices(2)
    curr3 = coordIndices(3)
    curr4 = coordIndices(4)
  }

  override def toString: String = {
    curr0 + "," + curr1 + "," + curr2 + "," + curr3 + "," + curr4
  }

  override def getCoordIndices: Array[Int] = {
    coordIndices(0) = curr0
    coordIndices(1) = curr1
    coordIndices(2) = curr2
    coordIndices(3) = curr3
    coordIndices(4) = curr4
    coordIndices.clone
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

  override def set0(v: Int): CDIndexIterator5D = {
    if (v < 0 || v >= shape0) throw new Exception()
    curr0 = v
    this
  }

  override def set1(v: Int): CDIndexIterator5D = {
    if (v < 0 || v >= shape1) throw new Exception()
    curr1 = v
    this
  }

  override def set2(v: Int): CDIndexIterator5D = {
    if (v < 0 || v >= shape2) throw new Exception()
    curr2 = v
    this
  }

  override def set3(v: Int): CDIndexIterator5D = {
    if (v < 0 || v >= shape3) throw new Exception()
    curr3 = v
    this
  }

  override def set4(v: Int): CDIndexIterator5D = {
    if (v < 0 || v >= shape4) throw new Exception()
    curr4 = v
    this
  }

  override def setCoordIndices(index: Array[Int]): CDIndexIterator5D = {
    if (index.length != rank) throw new Exception()
    set0(index(0))
    set1(index(1))
    set2(index(2))
    set3(index(3))
    set4(index(4))
    this
  }

  override def set(v0: Int, v1: Int, v2: Int, v3: Int, v4: Int): CDIndexIterator5D = {
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













