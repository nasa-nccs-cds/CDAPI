// Based on ucar.ma2.IndexIterator, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

package nasa.nccs.cdapi.tensors

object CDIterator {

  def factory( shape: Array[Int] ): CDArrayIndexIterator = factory( CDCoordIndex.factory(shape) )

  def factory( cdIndex: CDCoordIndex ): CDArrayIndexIterator = cdIndex.getRank match {
    case 1 =>
      return new CDIndexIterator1D(cdIndex)
    case 2 =>
      return new CDIndexIterator2D(cdIndex)
    case 3 =>
      return new CDIndexIterator3D(cdIndex)
    case 4 =>
      return new CDIndexIterator4D(cdIndex)
    case 5 =>
      return new CDIndexIterator5D(cdIndex)
    case _ =>
      return new CDArrayIndexIterator(cdIndex)
  }
}

abstract class CDIterator( _cdIndex: CDCoordIndex  ) extends collection.Iterator[Int] {
  protected val cdIndex: CDCoordIndex = CDCoordIndex.factory( _cdIndex )
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
  def mapToSection( index: Int ): Int = { setCurrentCounter(index); currentElement }

  protected def currentElement: Int = cdIndex.getStorageIndex( coordIndices )
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

class CDArrayIndexIterator( cdIndex: CDCoordIndex  ) extends CDIterator(cdIndex) {
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

class CDStorageIndexIterator( cdIndex: CDCoordIndex  ) extends CDIterator(cdIndex) {
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


class CDIndexIterator1D( cdIndex: CDCoordIndex ) extends  CDArrayIndexIterator( cdIndex  ) {
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

  override def setCoordIndices(cdIndex: Array[Int]): CDIndexIterator1D = {
    if (cdIndex.length != rank) throw new Exception()
    set0(cdIndex(0))
    this
  }

  private def setDirect(v0: Int): Int = {
    if (v0 < 0 || v0 >= shape0) throw new Exception()
    offset + v0 * stride0
  }
}

class CDIndexIterator2D( cdIndex: CDCoordIndex ) extends  CDArrayIndexIterator( cdIndex  ) {
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

  override def setCoordIndices( coordIndices: Array[Int]): CDIndexIterator2D = {
    if (coordIndices.length != rank) throw new Exception()
    set0(coordIndices(0))
    set1(coordIndices(1))
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


class CDIndexIterator3D( index: CDCoordIndex ) extends  CDArrayIndexIterator( index  ) {
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

class CDIndexIterator4D( index: CDCoordIndex ) extends  CDArrayIndexIterator( index  ) {
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


class CDIndexIterator5D( index: CDCoordIndex ) extends  CDArrayIndexIterator( index  ) {
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

object DualArrayIterator {
  def apply( input0: CDFloatArray, input1: CDFloatArray ): DualArrayIterator = {
    assert( input0.getRank == input1.getRank, "Can't combine arrays with different ranks")
    val sameShape = input0.getShape.sameElements(input1.getShape)
    val shape: Array[Int] = if(sameShape) input0.getShape else ( for( iS <- (0 until input0.getRank); s0 = input0.getShape(iS); s1 = input1.getShape(iS) )  yield
      if ( s0 == s1 ) s0
      else if ( s0 == 1 ) s1
      else if ( s1 == 1 ) s0
      else throw new Exception( "Attempt to combine incummensurate shapes: (%s) vs (%s)".format( input0.getShape.mkString(","), input1.getShape.mkString(",") ) ) ).toArray

    val cdIndex = CDCoordIndex.factory(shape)
    val sameShape0 = input0.getShape.sameElements(shape)
    val sameShape1 = input1.getShape.sameElements(shape)
    val array0 = if(sameShape0) input0 else input0.broadcast(shape)
    val array1 = if(sameShape1) input1 else input1.broadcast(shape)
    new DualArrayIterator( array0, array1, cdIndex )
  }
}

class DualArrayIterator( val array0: CDFloatArray, val array1: CDFloatArray, cdIndex: CDCoordIndex ) extends CDArrayIndexIterator( cdIndex  ) {
  val sameStructure0 = checkArrayStructure( 0 )
  val sameStructure1 = checkArrayStructure( 1 )
  var storageIndex = 0

  def checkArrayStructure( array_index: Int ): Boolean = {
    val _array = array_index match { case 0 => array0; case 1 => array1 }
    assert(_array.getShape.sameElements(cdIndex.getShape), "Error, array has wrong shape in DualArrayIterator!")
    _array.getStride.sameElements( cdIndex.getStride )
  }
  override def incr: Int = { storageIndex = super.incr; storageIndex }

  def value0() = if(sameStructure0) array0.getStorageValue(storageIndex) else array0.getValue(coordIndices)
  def value1() = if(sameStructure1) array1.getStorageValue(storageIndex) else array1.getValue(coordIndices)

}



//  val iter = input0.getIterator
//  val result = for (flatIndex <- iter; v0 = input0.getFlatValue(flatIndex); v1 = if(sameStructure) input1.getFlatValue(flatIndex) else input1.getValue( iter.getCoordinateIndices ) ) yield
//  if (!input0.valid(v0)) input0.invalid else if (!input1.valid(v1)) input0.invalid else reductionOp(v0, v1)
//  new CDFloatArray( input0.getShape, result.toArray, input0.invalid )





