package nasa.nccs.cdapi.tensors
import scala.collection.mutable.ListBuffer
import java.nio._
import ucar.ma2

class CDArray[T]( private val indexCalc: CDIndex, private val storage: Array[T] )  {
  private val rank = indexCalc.getRank
  private val dataType = getDataType

  def getDataType: ma2.DataType = {
    storage.headOption match {
      case Some(elem) => elem.getClass.getCanonicalName match {
        case "float" => ma2.DataType.FLOAT
        case "int" => ma2.DataType.INT
        case "btye" => ma2.DataType.BYTE
        case "char" => ma2.DataType.CHAR
        case "short" => ma2.DataType.SHORT
        case "double" => ma2.DataType.DOUBLE
        case "long" => ma2.DataType.LONG
        case x => throw new Exception( "Unsupported elem type in CDArray: " + x)
      }
      case None => ma2.DataType.OPAQUE
    }
  }

  def isStorageCongruent: Boolean = ( indexCalc.getSize == storage.length ) && !indexCalc.broadcasted

  def getIndex: CDIndex = {
    return new CDIndex( this.indexCalc )
  }

  def getIterator: CDIterator = {
    return if(isStorageCongruent) new CDStorageIndexIterator( indexCalc ) else CDIterator.factory( indexCalc )
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

  def getStorage: Array[T] = storage

  def copySectionData: List[T] = {
    val lbuff = ListBuffer.empty[T]
    val iter = getIterator
    while( iter.hasNext ) { lbuff += getValue( iter.next() ) }
    lbuff.toList
  }

  def getSectionData: Array[T] = if( isStorageCongruent ) getStorage else copySectionData

  def section(ranges: List[ma2.Range]): CDArray[T] = {
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

  def section(origin: Array[Int], shape: Array[Int], strideOpt: Option[Array[Int]]=None): CDArray[T] =
    createView(indexCalc.section(createRanges(origin,shape,strideOpt)))

  def slice(dim: Int, value: Int): CDArray[T] = {
    val origin: Array[Int] = new Array[Int](rank)
    val shape: Array[Int] = getShape
    origin(dim) = value
    shape(dim) = 1
    section(origin, shape).reduce(dim)
  }

  def flip(dim: Int): CDArray[T] = {
    return createView(indexCalc.flip(dim))
  }

  def transpose(dim1: Int, dim2: Int): CDArray[T] = {
    return createView(indexCalc.transpose(dim1, dim2))
  }

  def permute(dims: Array[Int]): CDArray[T] = {
    return createView(indexCalc.permute(dims))
  }

  def reshape(shape: Array[Int]): CDArray[T] = {
    if( shape.product != getSize ) throw new IllegalArgumentException("reshape arrays must have same total size")
    new CDArray( new CDIndex(shape), getSectionData )
  }


  def reduce: CDArray[T] = {
    val ri: CDIndex = indexCalc.reduce
    if (ri eq indexCalc) return this
    return createView(ri)
  }

  def reduce(dim: Int): CDArray[T] = {
    return createView(indexCalc.reduce(dim))
  }

  def isVlen: Boolean = {
    return false
  }

  protected def createView( index: CDIndex ): CDArray[T] = {
    return new CDArray[T](index, storage)
  }

  def getDataAsByteBuffer: ByteBuffer = {
    val bb: ByteBuffer = ByteBuffer.allocate( ( dataType.getSize * getSize ).asInstanceOf[Int] )
    dataType match {
      case ma2.DataType.FLOAT => bb.asFloatBuffer.put( getSectionData.asInstanceOf[Array[Float]] )
      case ma2.DataType.INT => bb.asIntBuffer.put( getSectionData.asInstanceOf[Array[Int]] )
      case ma2.DataType.BYTE => bb.put( getSectionData.asInstanceOf[Array[Byte]] )
      case ma2.DataType.SHORT => bb.asShortBuffer.put( getSectionData.asInstanceOf[Array[Short]] )
      case ma2.DataType.DOUBLE => bb.asDoubleBuffer.put( getSectionData.asInstanceOf[Array[Double]] )
      case ma2.DataType.CHAR => bb.asCharBuffer.put( getSectionData.asInstanceOf[Array[Char]] )
      case ma2.DataType.LONG => bb.asLongBuffer.put( getSectionData.asInstanceOf[Array[Long]] )
    }
    return bb
  }

  def getValue(index: Int): T = {
    return storage(index)
  }
}





