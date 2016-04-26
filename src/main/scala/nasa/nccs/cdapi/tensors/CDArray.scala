package nasa.nccs.cdapi.tensors
import java.nio._

import ucar.ma2

import scala.collection.GenTraversableOnce


object CDArray {

  def factory[T <: AnyVal]( shape: Array[Int], storage: Array[T] ): CDArray[T] = factory( new CDIndex(shape), storage )

  def factory[T <: AnyVal]( index: CDIndex, storage: Array[T] ): CDArray[T] = {
    CDArray.getDataType(storage) match {
      case ma2.DataType.FLOAT => new CDFloatArray( index, storage.asInstanceOf[Array[Float]] ).asInstanceOf[CDArray[T]]
      case ma2.DataType.INT => new CDIntArray( index, storage.asInstanceOf[Array[Int]] ).asInstanceOf[CDArray[T]]
      case ma2.DataType.BYTE => new CDByteArray( index, storage.asInstanceOf[Array[Byte]] ).asInstanceOf[CDArray[T]]
      case ma2.DataType.SHORT => new CDShortArray( index, storage.asInstanceOf[Array[Short]] ).asInstanceOf[CDArray[T]]
      case ma2.DataType.DOUBLE => new CDDoubleArray( index, storage.asInstanceOf[Array[Double]] ).asInstanceOf[CDArray[T]]
      case x => throw new Exception( "Unsupported elem type in CDArray: " + x)
    }
  }

  def factory[T <: AnyVal]( array: ucar.ma2.Array ): CDArray[T] =
    factory( new CDIndex( array.getShape ), array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[T]] )

  def getDataType[T <: AnyVal]( storage: Array[T] ): ma2.DataType = {
    storage.headOption match {
      case Some(elem) => elem.getClass.getSimpleName.toLowerCase match {
        case "float" => ma2.DataType.FLOAT
        case "int" => ma2.DataType.INT
        case "btye" => ma2.DataType.BYTE
        case "short" => ma2.DataType.SHORT
        case "double" => ma2.DataType.DOUBLE
        case x => throw new Exception( "Unsupported elem type in CDArray: " + x)
      }
      case None => ma2.DataType.OPAQUE
    }
  }

}

abstract class CDArray[ T <: AnyVal ]( private val indexCalc: CDIndex, private val storage: Array[T] )  {
  protected val rank = indexCalc.getRank
  protected val dataType = CDArray.getDataType(storage)

  def isStorageCongruent: Boolean = ( indexCalc.getSize == storage.length ) && !indexCalc.broadcasted
  def getIndex: CDIndex = new CDIndex( this.indexCalc )
  def dup() = CDArray.factory[T]( indexCalc, storage )
  def getIterator: CDIterator = return if(isStorageCongruent) new CDStorageIndexIterator( indexCalc ) else CDIterator.factory( indexCalc )
  def getRank: Int = rank
  def getShape: Array[Int] = indexCalc.getShape
  def getFlatValue( index: Int ): T = storage(index)
  def getValue( indices: Array[Int] ): T = storage( indexCalc.getFlatIndex(indices) )
  def setFlatValue( index: Int, value: T ): Unit = { storage(index) = value }
  def setValue( indices: Array[Int], value: T ): Unit = { storage( indexCalc.getFlatIndex(indices) ) = value }
  def getSize: Int =  indexCalc.getSize
  def getSizeBytes: Int =  indexCalc.getSize * dataType.getSize
  def getRangeIterator(ranges: List[ma2.Range] ): CDIterator = section(ranges).getIterator
  def getStorage: Array[T] = storage
  def map[B<: AnyVal](f:(T)=>B): CDArray[B] = CDArray.factory[B]( indexCalc, storage.map(f).asInstanceOf[Array[B]] )
  def flatMap[B<: AnyVal](f:(T)=>GenTraversableOnce[B]): CDArray[B] = CDArray.factory[B]( indexCalc, storage.flatMap(f).asInstanceOf[Array[B]] )
  def copySectionData: Array[T]
  def getSectionData: Array[T] = if( isStorageCongruent ) getStorage else copySectionData
  def section(ranges: List[ma2.Range]): CDArray[T] = createView(indexCalc.section(ranges))

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
    section(origin, shape)
  }

  def broadcast(dim: Int, size: Int ): CDArray[T] = createView( indexCalc.broadcast(dim,size) )
  def broadcast(shape: Array[Int] ): CDArray[T] = createView( indexCalc.broadcast(shape) )
  def flip(dim: Int): CDArray[T] = createView(indexCalc.flip(dim))
  def transpose(dim1: Int, dim2: Int): CDArray[T] = createView(indexCalc.transpose(dim1, dim2))
  def permute(dims: Array[Int]): CDArray[T] = createView(indexCalc.permute(dims))

  def reshape(shape: Array[Int]): CDArray[T] = {
    if( shape.product != getSize ) throw new IllegalArgumentException("reshape arrays must have same total size")
    CDArray.factory( new CDIndex(shape), getSectionData )
  }

  def reduce: CDArray[T] = {
    val ri: CDIndex = indexCalc.reduce
    if (ri eq indexCalc) return this
    createView(ri)
  }

  def reduce(dim: Int): CDArray[T] = createView(indexCalc.reduce(dim))
  def isVlen: Boolean = false

  protected def createView( index: CDIndex ): CDArray[T] = CDArray.factory(index, storage)

  def getDataAsByteBuffer: ByteBuffer = {
    val bb: ByteBuffer = ByteBuffer.allocate( ( dataType.getSize * getSize ).asInstanceOf[Int] )
    dataType match {
      case ma2.DataType.FLOAT => bb.asFloatBuffer.put( getSectionData.asInstanceOf[Array[Float]] )
      case ma2.DataType.INT => bb.asIntBuffer.put( getSectionData.asInstanceOf[Array[Int]] )
      case ma2.DataType.BYTE => bb.put( getSectionData.asInstanceOf[Array[Byte]] )
      case ma2.DataType.SHORT => bb.asShortBuffer.put( getSectionData.asInstanceOf[Array[Short]] )
      case ma2.DataType.DOUBLE => bb.asDoubleBuffer.put( getSectionData.asInstanceOf[Array[Double]] )
      case x => throw new Exception( "Unsupported elem type in CDArray: " + x)
    }
    bb
  }

  override def toString: String = {
    "Index: " + this.indexCalc.toString + "\n Data = " + getSectionData.mkString("[ ",", "," ]")
  }
}

object CDFloatArray {

  implicit def cdArrayConverter( target: CDArray[Float] ): CDFloatArray = new CDFloatArray( target.getIndex, target.getStorage )
  implicit def toUcarArray( target: CDFloatArray ): ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, target.getShape, target.getSectionData )

  def factory(array: ucar.ma2.Array): CDFloatArray = {
    val array_data = array.get1DJavaArray(array.getElementType).asInstanceOf[Array[AnyVal]]
    val storage: Array[Float] = array.getElementType.getSimpleName.toLowerCase match {
      case "float" => array_data.asInstanceOf[Array[Float]]
      case "int" => array_data.asInstanceOf[Array[Int]].map(_.toFloat)
      case "short" => array_data.asInstanceOf[Array[Short]].map(_.toFloat)
      case "byte" => array_data.asInstanceOf[Array[Byte]].map(_.toFloat)
      case "double" => array_data.asInstanceOf[Array[Double]].map(_.toFloat)
      case x => throw new Exception("Unsupported elem type in CDArray: " + x)
    }
    new CDFloatArray(new CDIndex(array.getShape), storage)
  }

  def combine( op: TensorCombinerOp, input0: CDFloatArray, input1: CDFloatArray ): CDFloatArray = {
    assert( input0.getShape == input1.getShape, "Can't combine arrays with different shapes")
    val iter = input0.getIterator
    val result = for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex); v1 = input1.getFlatValue(flatIndex) ) yield op.combine(v0,v1)
    new CDFloatArray( input0.getShape, result.toArray, input0.invalid )
  }

  def icombine( op: TensorCombinerOp, input0: CDFloatArray, input1: CDFloatArray ): Unit = {
    assert( input0.getShape == input1.getShape, "Can't combine arrays with different shapes")
    val iter = input0.getIterator
    for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex); v1 = input1.getFlatValue(flatIndex) ) input0.setFlatValue( flatIndex,  op.combine(v0,v1) )
  }

  def combine( op: TensorCombinerOp, input0: CDFloatArray, fval: Float ): CDFloatArray = {
    val iter = input0.getIterator
    val result = for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex) ) yield op.combine(v0,fval)
    new CDFloatArray( input0.getShape, result.toArray, input0.invalid )
  }

  def icombine( op: TensorCombinerOp, input0: CDFloatArray, fval: Float ): Unit = {
    val iter = input0.getIterator
    val result = for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex) ) input0.setFlatValue( flatIndex, op.combine(v0,fval) )
  }

  def icombine( op: TensorCombinerOp, input0: CDFloatArray, input1: CDFloatArray, countArray: CDFloatArray ): Unit = {
    val iter = input0.getIterator
    for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex); if input0.valid(v0); v1 = input1.getFlatValue(flatIndex); if input1.valid(v0) )  {
       input0.setFlatValue(flatIndex, op.combine( v0, v1) )
       countArray.setFlatValue( flatIndex, countArray.getFlatValue( flatIndex ) + 1 )
    }
  }

  def accumulate( op: TensorAccumulatorOp, input0: CDFloatArray, input1: CDFloatArray ): Array[Float] = {
    assert( input0.getShape == input1.getShape, "Can't combine arrays with different shapes")
    val iter = input0.getIterator
    for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex); if input0.valid(v0); v1 = input1.getFlatValue(flatIndex) ) op.insert(v0,v1)
    op.getResult
  }

  def accumulate( op: TensorAccumulatorOp, input0: CDFloatArray, fval: Float ): Array[Float] = {
    val iter = input0.getIterator
    for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex); if input0.valid(v0) ) op.insert(v0,fval)
    op.getResult
  }

}

class CDFloatArray( indexCalc: CDIndex, storage: Array[Float], val invalid: Float = Float.MaxValue ) extends CDArray[Float](indexCalc,storage) {

  def this( shape: Array[Int], storage: Array[Float], invalid: Float ) = this( CDIndex.factory(shape), storage, invalid )
  def this( shape: Array[Int], storage: Array[Float] ) = this( CDIndex.factory(shape), storage )

  def getData: Array[Float] = storage.asInstanceOf[Array[Float]]

  override def dup(): CDFloatArray = new CDFloatArray( indexCalc.getShape, this.getSectionData )

  def valid( value: Float ) = ( value != invalid )

  def toCDFloatArray( target: CDArray[Float] ) = new CDFloatArray( target.getIndex, target.getStorage, invalid )

  def copySectionData: Array[Float] = {
    val iter = getIterator
    val array_data_iter = for ( index <- iter; value = getData(index) ) yield { value }
    array_data_iter.toArray
  }
  def zeros: CDFloatArray = new CDFloatArray( getShape, Array.fill[Float]( getSize )(0), invalid )
  def invalids: CDFloatArray = new CDFloatArray( getShape, Array.fill[Float]( getSize )(invalid), invalid )

  def -(array: CDFloatArray) = CDFloatArray.combine( subOp(invalid), this, array )
  def -=(array: CDFloatArray) = CDFloatArray.icombine( subOp(invalid), this, array )
  def +(array: CDFloatArray) = CDFloatArray.combine( addOp(invalid), this, array )
  def +=(array: CDFloatArray) = CDFloatArray.icombine( addOp(invalid), this, array )
  def /(array: CDFloatArray) = CDFloatArray.combine( divOp(invalid), this, array )
  def *(array: CDFloatArray) = CDFloatArray.combine( multOp(invalid), this, array )
  def /=(array: CDFloatArray) = CDFloatArray.icombine( divOp(invalid), this, array )
  def *=(array: CDFloatArray) = CDFloatArray.icombine( multOp(invalid), this, array )
  def -(value: Float) = CDFloatArray.combine( subOp(invalid), this, value )
  def +(value: Float) = CDFloatArray.combine( addOp(invalid), this, value )
  def -=(value: Float) = CDFloatArray.icombine( subOp(invalid), this, value )
  def +=(value: Float) = CDFloatArray.icombine( addOp(invalid), this, value )
  def /(value: Float) = CDFloatArray.combine( divOp(invalid), this, value )
  def *(value: Float) = CDFloatArray.combine( multOp(invalid), this, value )
  def /=(value: Float) = CDFloatArray.icombine( divOp(invalid), this, value )
  def *=(value: Float) = CDFloatArray.icombine( multOp(invalid), this, value )
  def ++=(values: CDFloatArray, counts: CDFloatArray) = CDFloatArray.icombine( addOp(invalid), this, values, counts )

  def mean( weightsOpt: Option[CDFloatArray], dimensions: Int* ): CDFloatArray = execAccumulatorOp( new meanOp(invalid), weightsOpt, dimensions:_* )

  def computeWeights( weighting_type: String, axisDataMap: Map[ Char, ( Int, ma2.Array ) ] ) : CDFloatArray  = {
    weighting_type match {
      case "cosine" =>
        axisDataMap.get('y') match {
          case Some( ( axisIndex, yAxisData ) ) =>
            val axis_length = yAxisData.getSize
            val axis_data = CDFloatArray.factory( yAxisData )
            assert( axis_length == getShape(axisIndex), "Y Axis data mismatch, %d vs %d".format(axis_length,getShape(axisIndex) ) )
            val cosineWeights: CDArray[Float] = axis_data.map( x => Math.cos( Math.toRadians(x) ).toFloat )
            val base_shape: Array[Int] = Array( (0 until rank).map(i => if(i==axisIndex) getShape(axisIndex) else 1 ): _* )
            val weightsArray: CDArray[Float] =  CDArray.factory( base_shape, cosineWeights.getStorage )
            weightsArray.broadcast( getShape )
          case None => throw new NoSuchElementException( "Missing axis data in weights computation, type: %s".format( weighting_type ))
        }
      case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
    }
  }

}

class CDByteArray( indexCalc: CDIndex, storage: Array[Byte] ) extends CDArray[Byte](indexCalc,storage) {

  def getData: Array[Byte] = storage.asInstanceOf[Array[Byte]]

  def this( shape: Array[Int], storage: Array[Byte] ) = this( CDIndex.factory(shape), storage )

  override def dup(): CDByteArray = new CDByteArray( indexCalc.getShape, this.getSectionData )

  def copySectionData: Array[Byte] = {
    val iter = getIterator
    val array_data_iter = for (index <- iter; value = getData(index)) yield value
    array_data_iter.toArray
  }
}

class CDIntArray( indexCalc: CDIndex, storage: Array[Int] ) extends CDArray[Int](indexCalc,storage) {

  def getData: Array[Int] = storage.asInstanceOf[Array[Int]]

  def this( shape: Array[Int], storage: Array[Int] ) = this( CDIndex.factory(shape), storage )

  override def dup(): CDIntArray = new CDIntArray( indexCalc.getShape, this.getSectionData )

  def copySectionData: Array[Int] = {
    val iter = getIterator
    val array_data_iter = for ( index <- iter; value = getData(index) ) yield value
    array_data_iter.toArray
  }
}

class CDShortArray( indexCalc: CDIndex, storage: Array[Short] ) extends CDArray[Short](indexCalc,storage) {

  def getData: Array[Short] = storage.asInstanceOf[Array[Short]]

  def this( shape: Array[Int], storage: Array[Short] ) = this( CDIndex.factory(shape), storage )

  override def dup(): CDShortArray = new CDShortArray( indexCalc.getShape, this.getSectionData )

  def copySectionData: Array[Short] = {
    val iter = getIterator
    val array_data_iter = for ( index <- iter; value = getData(index) ) yield value
    array_data_iter.toArray
  }
}

object CDDoubleArray {
  implicit def cdArrayConverter(target: CDArray[Double]): CDDoubleArray = new CDDoubleArray(target.getIndex, target.getStorage)
  implicit def toUcarArray(target: CDDoubleArray): ma2.Array = ma2.Array.factory(ma2.DataType.DOUBLE, target.getShape, target.getSectionData)
}

class CDDoubleArray( indexCalc: CDIndex, storage: Array[Double], val invalid: Double = Double.MaxValue ) extends CDArray[Double](indexCalc,storage) {

  def getData: Array[Double] = storage.asInstanceOf[Array[Double]]

  def this( shape: Array[Int], storage: Array[Double] ) = this( CDIndex.factory(shape), storage )
  def this( shape: Array[Int], storage: Array[Double], invalid: Double ) = this( CDIndex.factory(shape), storage, invalid )

  def valid( value: Double ) = ( value != invalid )

  override def dup(): CDDoubleArray = new CDDoubleArray( indexCalc.getShape, this.getSectionData )

  def copySectionData: Array[Double] = {
    val iter = getIterator
    val array_data_iter = for ( index <- iter; value = getData(index) ) yield value
    array_data_iter.toArray
  }

  def zeros: CDDoubleArray = new CDDoubleArray( getShape, Array.fill[Double]( getSize )(0), invalid )
  def invalids: CDDoubleArray = new CDDoubleArray( getShape, Array.fill[Double]( getSize )(invalid), invalid )

}


object ArrayTest extends App {
  val base_shape = Array(5,5,5)
  val subset_origin = Array(1,1,1)
  val subset_shape = Array(2,2,2)
  val storage = Array.iterate( 0f, 125 )( x => x + 1f )
  val cdIndex: CDIndex = CDIndex.factory( base_shape )
  val cd_array = CDArray.factory( cdIndex, storage )
  val ma2_array: ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, base_shape, storage )
  val cd_array_subset = cd_array.section( subset_origin,  subset_shape )
  val ma2_array_subset = ma2_array.sectionNoReduce( subset_origin,  subset_shape, Array(1,1,1) )

  val cd_array_slice = cd_array_subset.slice( 0, 0 )
  val ma2_array_slice = ma2_array_subset.slice( 0, 0 ).reshape( Array(1,2,2) )

  val cd_array_tp = cd_array_slice.transpose( 1,2 )
  val ma2_array_tp = ma2_array_slice.transpose( 1,2 )


  println( cd_array_tp.toString )
  println( ma2_array_tp.toString )

//  val cd_array_bcast = cd_array_slice.broadcast( 0, 3 )
//  println( cd_array_bcast.toString )
}

object ArrayPerformanceTest extends App {
  import scala.collection.mutable.ListBuffer
  val base_shape = Array(100,100,10)
  val storage: Array[Float] = Array.iterate( 0f, 100000 )( x => x + 1f )
  val cdIndex: CDIndex = CDIndex.factory( base_shape )
  val cd_array = new CDFloatArray( cdIndex, storage )
  val ma2_array: ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, base_shape, storage )

  val cdIter = cd_array.getIterator
  val ma2Iter: ma2.IndexIterator = ma2_array.getIndexIterator

  val itest = 1

  itest match {
    case 0 =>
      val t00 = System.nanoTime
      val cdResult = for (index <- cdIter; value = cd_array.getData (index) ) yield { value * value }
      val cdResultData = cdResult.toArray
      val t01 = System.nanoTime
      println ("cd2Result Time = %.4f,  ".format ((t01 - t00) / 1.0E9) )

      var result = new ListBuffer[Float] ()
      val t10 = System.nanoTime
      val ma2Result = while (ma2Iter.hasNext) { val value = ma2Iter.getFloatNext; result += value * value }
      val ma2ResultData = result.toArray
      val t11 = System.nanoTime
      println ("ma2Result Time = %.4f,  ".format ((t11 - t10) / 1.0E9) )

    case 1 =>
      val section_origin = Array( 10, 10, 1 )
      val section_shape = Array( 80, 80, 8 )
      val section_strides = Array( 1, 1, 1 )

      val t00 = System.nanoTime
      val cdResult = cd_array.section( section_origin, section_shape )
      val cdData = cdResult.getDataAsByteBuffer
      val t01 = System.nanoTime
      println ("cd2Result Time = %.4f,  ".format ((t01 - t00) / 1.0E9) )

      val t10 = System.nanoTime
      val ma2Result = ma2_array.section( section_origin, section_shape )
      val ma2Data = ma2Result.getDataAsByteBuffer
      val t11 = System.nanoTime
      println ("ma2Result Time = %.4f,  ".format ((t11 - t10) / 1.0E9) )
  }



}






