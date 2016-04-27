// Based on ucar.ma2.Array, portions of which were developed by the Unidata Program at the University Corporation for Atmospheric Research.

package nasa.nccs.cdapi.tensors
import java.nio._
import ucar.ma2

object CDArray {

  def factory[T <: AnyVal]( shape: Array[Int], storage: Array[T] ): CDArray[T] = factory( new CDCoordIndex(shape), storage )

  def factory[T <: AnyVal]( index: CDCoordIndex, storage: Array[T] ): CDArray[T] = {
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
    factory( new CDCoordIndex( array.getShape ), array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[T]] )

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

abstract class CDArray[ T <: AnyVal ]( private val cdIndex: CDCoordIndex, private val storage: Array[T] )  {
  protected val rank = cdIndex.getRank
  protected val dataType = CDArray.getDataType(storage)

  def isStorageCongruent: Boolean = ( cdIndex.getSize == storage.length ) && !cdIndex.broadcasted
  def getIndex: CDCoordIndex = new CDCoordIndex( this.cdIndex )
  def dup() = CDArray.factory[T]( cdIndex, storage )
  def getIterator: CDIterator = if(isStorageCongruent) new CDStorageIndexIterator( cdIndex ) else CDIterator.factory( cdIndex )
  def getRank: Int = rank
  def getShape: Array[Int] = cdIndex.getShape
  def getReducedShape: Array[Int] = cdIndex.getReducedShape
  def getFlatValue( index: Int ): T = storage(index)
  def getValue( indices: Array[Int] ): T = storage( cdIndex.getFlatIndex(indices) )
  def setFlatValue( index: Int, value: T ): Unit = { storage(index) = value }
  def setValue( indices: Array[Int], value: T ): Unit = { storage( cdIndex.getFlatIndex(indices) ) = value }
  def getSize: Int =  cdIndex.getSize
  def getSizeBytes: Int =  cdIndex.getSize * dataType.getSize
  def getRangeIterator(ranges: List[ma2.Range] ): CDIterator = section(ranges).getIterator
  def getStorage: Array[T] = storage
  def map[B<: AnyVal](f:(T)=>B): CDArray[B] = CDArray.factory[B]( cdIndex, storage.map(f).asInstanceOf[Array[B]] )
//  def flatMap[B<: AnyVal](f:(T)=>GenTraversableOnce[B]): CDArray[B] = CDArray.factory[B]( cdIndex, storage.flatMap(f).asInstanceOf[Array[B]] )
  def copySectionData: Array[T]
  def getSectionData: Array[T] = if( isStorageCongruent ) getStorage else copySectionData
  def section(ranges: List[ma2.Range]): CDArray[T] = createView(cdIndex.section(ranges))
  def valid( value: T ): Boolean
  def spawn( shape: Array[Int], fillval: T ): CDArray[T]

  def getAccumulatorArray( reduceAxes: Array[Int], fillval: T, fullShape: Array[Int] = getShape ): CDArray[T] = {
    val reducedShape = for( idim <- ( 0 until rank) ) yield if( reduceAxes.contains(idim) ) 1 else fullShape( idim )
    val accumulator = spawn( reducedShape.toArray, fillval )
    accumulator.broadcast( fullShape )
  }

  def getReducedArray(): CDArray[T] = { CDArray.factory[T]( getReducedShape, storage ) }

  def reduce( reductionOp: (T,T)=>T, reduceDims: Array[Int], initVal: T, coordMapOpt: Option[CDCoordMap] = None ): CDArray[T] = {
    val fullShape = coordMapOpt match { case Some(coordMap) => coordMap.mapShape( getShape ); case None => getShape }
    val accumulator: CDArray[T] = getAccumulatorArray( reduceDims, initVal, fullShape )
    val iter = accumulator.getIterator
    coordMapOpt match {
      case Some(coordMap) =>
        for (index <- iter; array_value = getFlatValue(index); if valid(array_value); coordIndices = iter.getCoordinateIndices) {
          val mappedCoords = coordMap.map(coordIndices)
          accumulator.setValue(mappedCoords, reductionOp(accumulator.getValue(mappedCoords), array_value))
        }
      case None =>
        for (index <- iter; array_value = getFlatValue(index); if valid(array_value); coordIndices = iter.getCoordinateIndices)
          accumulator.setValue(coordIndices, reductionOp(accumulator.getValue(coordIndices), array_value))
    }
    accumulator.getReducedArray
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
    createView(cdIndex.section(createRanges(origin,shape,strideOpt)))

  def slice(dim: Int, value: Int): CDArray[T] = {
    val origin: Array[Int] = new Array[Int](rank)
    val shape: Array[Int] = getShape
    origin(dim) = value
    shape(dim) = 1
    section(origin, shape)
  }

  def broadcast(dim: Int, size: Int ): CDArray[T] = createView( cdIndex.broadcast(dim,size) )
  def broadcast(shape: Array[Int] ): CDArray[T] = createView( cdIndex.broadcast(shape) )
  def flip(dim: Int): CDArray[T] = createView(cdIndex.flip(dim))
  def transpose(dim1: Int, dim2: Int): CDArray[T] = createView(cdIndex.transpose(dim1, dim2))
  def permute(dims: Array[Int]): CDArray[T] = createView(cdIndex.permute(dims))

  def reshape(shape: Array[Int]): CDArray[T] = {
    if( shape.product != getSize ) throw new IllegalArgumentException("reshape arrays must have same total size")
    CDArray.factory( new CDCoordIndex(shape), getSectionData )
  }

  def reduce: CDArray[T] = {
    val ri: CDCoordIndex = cdIndex.reduce
    if (ri eq cdIndex) return this
    createView(ri)
  }

  def reduce(dim: Int): CDArray[T] = createView(cdIndex.reduce(dim))
  def isVlen: Boolean = false

  protected def createView( cdIndex: CDCoordIndex ): CDArray[T] = CDArray.factory(cdIndex, storage)

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
    "Index: " + this.cdIndex.toString + "\n Data = " + getSectionData.mkString("[ ",", "," ]")
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
    new CDFloatArray(new CDCoordIndex(array.getShape), storage)
  }

  def spawn( shape: Array[Int], f: (Array[Int]) => Float ): CDFloatArray = {
    val new_array: CDFloatArray = CDArray.factory( shape, Array.fill[Float]( shape.product )(0f)  )
    val cdIndex = new CDCoordIndex( shape )
    val iter = CDIterator.factory( cdIndex )
    for ( index <- iter; coords = iter.getCoordinateIndices; value = f(coords) ) new_array.setValue( coords, value )
    new_array
  }

  def combine( reductionOp: (Float,Float)=>Float, input0: CDFloatArray, input1: CDFloatArray ): CDFloatArray = {
    assert( input0.getShape.sameElements(input1.getShape), "Can't combine arrays with different shapes: (%s) vs (%s)".format( input0.getShape.mkString(","), input1.getShape.mkString(",")))
    val iter = input0.getIterator
    val result = for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex); v1 = input1.getFlatValue(flatIndex) ) yield
      if( !input0.valid( v0 ) ) input0.invalid
      else if ( !input1.valid( v1 ) ) input0.invalid
      else reductionOp(v0,v1)
    new CDFloatArray( input0.getShape, result.toArray, input0.invalid )
  }

  def accumulate( reductionOp: (Float,Float)=>Float, input0: CDFloatArray, input1: CDFloatArray ): Unit = {
    assert( input0.getShape.sameElements(input1.getShape), "Can't combine arrays with different shapes: (%s) vs (%s)".format( input0.getShape.mkString(","), input1.getShape.mkString(",")))
    val iter = input0.getIterator
    for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex); if(input0.valid(v0)); v1 = input1.getFlatValue(flatIndex); if(input1.valid(v1)) )
      input0.setFlatValue( flatIndex,  reductionOp(v0,v1) )
  }

  def combine( reductionOp: (Float,Float)=>Float, input0: CDFloatArray, fval: Float ): CDFloatArray = {
    val iter = input0.getIterator
    val result = for( flatIndex <- iter; v0 = input0.getFlatValue(flatIndex) ) yield
      if( !input0.valid( v0 ) ) input0.invalid
      else reductionOp(v0,fval)
    new CDFloatArray( input0.getShape, result.toArray, input0.invalid )
  }

}

class CDFloatArray( cdIndex: CDCoordIndex, storage: Array[Float], val invalid: Float = Float.MaxValue ) extends CDArray[Float](cdIndex,storage) {

  def this( shape: Array[Int], storage: Array[Float], invalid: Float ) = this( CDCoordIndex.factory(shape), storage, invalid )
  def this( shape: Array[Int], storage: Array[Float] ) = this( CDCoordIndex.factory(shape), storage )

  def getData: Array[Float] = storage.asInstanceOf[Array[Float]]

  override def dup(): CDFloatArray = new CDFloatArray( cdIndex.getShape, this.getSectionData )

  def valid( value: Float ) = ( value != invalid )

  def toCDFloatArray( target: CDArray[Float] ) = new CDFloatArray( target.getIndex, target.getStorage, invalid )

  def copySectionData: Array[Float] = {
    val iter = getIterator
    val array_data_iter = for ( index <- iter; value = getData(index) ) yield { value }
    array_data_iter.toArray
  }

  def spawn( shape: Array[Int], fillval: Float ): CDArray[Float] = CDArray.factory( shape, Array.fill[Float]( shape.product )(fillval)  )
  def zeros: CDFloatArray = new CDFloatArray( getShape, Array.fill[Float]( getSize )(0), invalid )
  def invalids: CDFloatArray = new CDFloatArray( getShape, Array.fill[Float]( getSize )(invalid), invalid )


  def -(array: CDFloatArray) = CDFloatArray.combine( (x:Float, y:Float) => ( x - y ), this, array )
  def -=(array: CDFloatArray) = CDFloatArray.accumulate( (x:Float, y:Float) => ( x - y ), this, array )
  def +(array: CDFloatArray) = CDFloatArray.combine( (x:Float, y:Float) => ( x + y ), this, array )
  def +=(array: CDFloatArray) = CDFloatArray.accumulate( (x:Float, y:Float) => ( x + y ), this, array )
  def /(array: CDFloatArray) = CDFloatArray.combine( (x:Float, y:Float) => ( x / y ), this, array )
  def *(array: CDFloatArray) = CDFloatArray.combine( (x:Float, y:Float) => ( x * y ), this, array )
  def /=(array: CDFloatArray) = CDFloatArray.accumulate( (x:Float, y:Float) => ( x / y ), this, array )
  def *=(array: CDFloatArray) = CDFloatArray.accumulate( (x:Float, y:Float) => ( x * y ), this, array )
  def -(value: Float) = CDFloatArray.combine( (x:Float, y:Float) => ( x - y ), this, value )
  def +(value: Float) = CDFloatArray.combine( (x:Float, y:Float) => ( x + y ), this, value )
  def /(value: Float) = CDFloatArray.combine( (x:Float, y:Float) => ( x / y ), this, value )
  def *(value: Float) = CDFloatArray.combine( (x:Float, y:Float) => ( x * y ), this, value )

  def max(reduceDims: Array[Int]): CDFloatArray = reduce( (x:Float, y:Float) => ( if( x > y ) x else y ), reduceDims, Float.MinValue )
  def min(reduceDims: Array[Int]): CDFloatArray = reduce( (x:Float, y:Float) => ( if( x < y ) x else y ), reduceDims, Float.MaxValue )
  def sum(reduceDims: Array[Int]): CDFloatArray = reduce( (x:Float, y:Float) => ( x + y ), reduceDims, 0f )

  def mean(reduceDims: Array[Int], weightsOpt: Option[CDFloatArray] = None): CDFloatArray = {
    if (weightsOpt.isDefined) assert( getShape.sameElements(weightsOpt.get.getShape), " Weight array in mean op has wrong shape: (%s) vs. (%s)".format( weightsOpt.get.getShape.mkString(","), getShape.mkString(",") ))
    val values_accumulator: CDFloatArray = getAccumulatorArray(reduceDims, 0f)
    val weights_accumulator: CDFloatArray = getAccumulatorArray(reduceDims, 0f)
    val iter = getIterator
    for (cdIndex <- iter; array_value = getData(cdIndex); if valid(array_value); coordIndices = iter.getCoordinateIndices) weightsOpt match {
      case Some(weights) =>
        val weight = weights.getValue(coordIndices);
        values_accumulator.setValue(coordIndices, values_accumulator.getValue(coordIndices) + array_value * weight)
        weights_accumulator.setValue(coordIndices, weights_accumulator.getValue(coordIndices) + weight)
      case None =>
        values_accumulator.setValue(coordIndices, values_accumulator.getValue(coordIndices) + array_value)
        weights_accumulator.setValue(coordIndices, weights_accumulator.getValue(coordIndices) + 1f)
    }
    val values_sum: CDFloatArray = values_accumulator.getReducedArray
    val weights_sum: CDFloatArray = weights_accumulator.getReducedArray
    values_sum / weights_sum
  }

  def anomaly( reduceDims: Array[Int], weightsOpt: Option[CDFloatArray] = None ): CDFloatArray = {
    val meanval = mean( reduceDims, weightsOpt )
    this - meanval
  }


//  def execAccumulatorOp(op: TensorAccumulatorOp, auxDataOpt: Option[CDFloatArray], dimensions: Int*): CDFloatArray = {
//    assert( dimensions.nonEmpty, "Must specify at least one dimension ('axes' arg) for this operation")
//    val filtered_shape: IndexedSeq[Int] = (0 until rank).map(x => if (dimensions.contains(x)) 1 else getShape(x))
//    val slices = auxDataOpt match {
//      case Some(auxData) => Nd4j.concat(0, (0 until filtered_shape.product).map(iS => { Nd4j.create(subset(iS, dimensions: _*).accumulate2( op, auxData.subset( iS, dimensions: _*).tensor )) }): _*)
//      case None =>  Nd4j.concat(0, (0 until filtered_shape.product).map(iS => Nd4j.create(subset(iS, dimensions: _*).accumulate(op))): _*)
//    }
//    new CDFloatArray( slices.reshape(filtered_shape: _* ), invalid )
//  }

//  def mean( weightsOpt: Option[CDFloatArray], dimensions: Int* ): CDFloatArray = execAccumulatorOp( new meanOp(invalid), weightsOpt, dimensions:_* )

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

class CDByteArray( cdIndex: CDCoordIndex, storage: Array[Byte] ) extends CDArray[Byte](cdIndex,storage) {

  def getData: Array[Byte] = storage.asInstanceOf[Array[Byte]]

  def this( shape: Array[Int], storage: Array[Byte] ) = this( CDCoordIndex.factory(shape), storage )

  def valid( value: Byte ): Boolean = true

  override def dup(): CDByteArray = new CDByteArray( cdIndex.getShape, this.getSectionData )
  def spawn( shape: Array[Int], fillval: Byte ): CDArray[Byte] = CDArray.factory( shape, Array.fill[Byte]( shape.product )(fillval)  )

  def copySectionData: Array[Byte] = {
    val iter = getIterator
    val array_data_iter = for (index <- iter; value = getData(index)) yield value
    array_data_iter.toArray
  }
}

class CDIntArray( cdIndex: CDCoordIndex, storage: Array[Int] ) extends CDArray[Int](cdIndex,storage) {

  def getData: Array[Int] = storage.asInstanceOf[Array[Int]]

  def this( shape: Array[Int], storage: Array[Int] ) = this( CDCoordIndex.factory(shape), storage )
  def valid( value: Int ): Boolean = true

  override def dup(): CDIntArray = new CDIntArray( cdIndex.getShape, this.getSectionData )
  def spawn( shape: Array[Int], fillval: Int ): CDArray[Int] = CDArray.factory( shape, Array.fill[Int]( shape.product )(fillval)  )

  def copySectionData: Array[Int] = {
    val iter = getIterator
    val array_data_iter = for ( index <- iter; value = getData(index) ) yield value
    array_data_iter.toArray
  }
}

class CDShortArray( cdIndex: CDCoordIndex, storage: Array[Short] ) extends CDArray[Short](cdIndex,storage) {

  def getData: Array[Short] = storage.asInstanceOf[Array[Short]]

  def this( shape: Array[Int], storage: Array[Short] ) = this( CDCoordIndex.factory(shape), storage )
  def valid( value: Short ): Boolean = true

  override def dup(): CDShortArray = new CDShortArray( cdIndex.getShape, this.getSectionData )
  def spawn( shape: Array[Int], fillval: Short ): CDArray[Short] = CDArray.factory( shape, Array.fill[Short]( shape.product )(fillval)  )

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

class CDDoubleArray( cdIndex: CDCoordIndex, storage: Array[Double], val invalid: Double = Double.MaxValue ) extends CDArray[Double](cdIndex,storage) {

  def getData: Array[Double] = storage.asInstanceOf[Array[Double]]

  def this( shape: Array[Int], storage: Array[Double] ) = this( CDCoordIndex.factory(shape), storage )
  def this( shape: Array[Int], storage: Array[Double], invalid: Double ) = this( CDCoordIndex.factory(shape), storage, invalid )

  def valid( value: Double ) = ( value != invalid )

  override def dup(): CDDoubleArray = new CDDoubleArray( cdIndex.getShape, this.getSectionData )
  def spawn( shape: Array[Int], fillval: Double ): CDArray[Double] = CDArray.factory( shape, Array.fill[Double]( shape.product )(fillval)  )

  def copySectionData: Array[Double] = {
    val iter = getIterator
    val array_data_iter = for ( index <- iter; value = getData(index) ) yield value
    array_data_iter.toArray
  }

  def zeros: CDDoubleArray = new CDDoubleArray( getShape, Array.fill[Double]( getSize )(0), invalid )
  def invalids: CDDoubleArray = new CDDoubleArray( getShape, Array.fill[Double]( getSize )(invalid), invalid )

}

object ArrayReduceTest extends App {
  val base_shape = Array(5,5,5)
  val cd_array = CDFloatArray.spawn( base_shape, (x) => ( 0.5f*x(1) ) )
  val mean0 = cd_array.mean( Array(1) )
  println( mean0.toString )
}


object ArrayTest extends App {
  val base_shape = Array(5,5,5)
  val subset_origin = Array(1,1,1)
  val subset_shape = Array(2,2,2)
  val storage = Array.iterate( 0f, 125 )( x => x + 1f )
  val cdIndex: CDCoordIndex = CDCoordIndex.factory( base_shape )
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
  val cdIndex: CDCoordIndex = CDCoordIndex.factory( base_shape )
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






