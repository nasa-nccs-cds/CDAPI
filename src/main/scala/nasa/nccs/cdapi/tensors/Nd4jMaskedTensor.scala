package nasa.nccs.cdapi.tensors

import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.cpu.NDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.INDArrayIndex

// import org.nd4s.Implicits._
import scala.collection.immutable.IndexedSeq
// import scala.language.implicitConversions
//import scala.collection.JavaConversions._
////import scala.collection.JavaConverters._

class Nd4jMaskedTensor( tensor: INDArray = new NDArray(), val invalid: Float = Float.NaN ) extends Serializable {
  val name: String = "Nd4jMaskedTensor"
  val shape = tensor.shape

  override def toString =  "%s[%s]: %s".format(name, shape.mkString(","), tensor.data().toString)
  def rows = tensor.rows
  def cols = tensor.columns
 // def apply = this
//  def apply(indexes: Int*) = tensor.get(indexes.toArray).toFloat
  def data: Array[Float] = tensor.data.asFloat

  def execOp(op: TensorOp, dimensions: Int*): INDArray = {
    val filtered_shape: IndexedSeq[Int] = (0 until shape.length).flatMap(x => if (dimensions.exists(_ == x)) None else Some(shape(x)))
    val slices = Nd4j.concat(0, (0 until filtered_shape.product).map(iS => Nd4j.create(subset(iS, dimensions: _*).applyOp(op))): _*)
    val new_shape = if (op.length == 1) filtered_shape else filtered_shape :+ op.length
    slices.setShape(new_shape: _*);
    slices.cleanup()
    slices
  }

  def dup: Nd4jMaskedTensor = { new Nd4jMaskedTensor( tensor.dup, invalid ) }

  def exec( op: TensorOp, dimensions: Int* ): Nd4jMaskedTensor = {
    val slices = execOp( op, dimensions:_* )
    new Nd4jMaskedTensor( slices, invalid )
  }

  def subset( index: Int, dimensions: Int*  ): Nd4jMaskedTensor = {
    new Nd4jMaskedTensor( tensor.tensorAlongDimension(index, dimensions:_*), invalid )
  }

  def applyOp( op: TensorOp ): Array[Float] = {
    op.init
    for( iC <- 0 until tensor.length )  {
      val v = tensor.getFloat(iC)
      if( v != invalid ) op.insert(v)
    }
    op.result
  }

  def mean( dimensions: Int* ): Nd4jMaskedTensor = exec( meanOp, dimensions:_* )

  def rawmean( dimensions: Int* ): Nd4jMaskedTensor =  new Nd4jMaskedTensor( tensor.mean(dimensions:_*), invalid )

  def apply( ranges: List[INDArrayIndex] ) = {
    val IndArray = tensor.get(ranges: _*)
    new Nd4jMaskedTensor(IndArray)
  }

  /*

  def zeros(shape: Int*) = new Nd4jMaskedTensor(Nd4j.create(shape: _*))

  def map(f: Double => Double) = new Nd4jMaskedTensor(tensor.map(p => f(p)))

  def put(value: Float, shape: Int*) = tensor.putScalar(shape.toArray, value)

  def +(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.add(array.tensor))

  def -(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.sub(array.tensor))

  def \(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.div(array.tensor))

  def /(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.div(array.tensor))

  def *(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.mul(array.tensor))

  /**
   * Masking operations
   */

  def <=(num: Float): Nd4jMaskedTensor = new Nd4jMaskedTensor(tensor.map(p => if (p < num) p else 0.0))

  def :=(num: Float) = new Nd4jMaskedTensor(tensor.map(p => if (p == num) p else 0.0))

  /**
   * Linear Algebra Operations
   */
  
  def **(array: Nd4jMaskedTensor) = new Nd4jMaskedTensor(tensor.dot(array.tensor))
  
  def div(num: Float): Nd4jMaskedTensor = new Nd4jMaskedTensor(tensor.div(num))

  /**
   * SliceableArray operations
   */

  def rows = tensor.rows
  
  def cols = tensor.columns

  def apply = this

  def apply(ranges: (Int, Int)*) = {
    val rangeMap = ranges.map(p => TupleRange(p))
    val indArray = tensor(rangeMap: _*)
    new Nd4jMaskedTensor(indArray,invalid)
  }

  def apply(indexes: Int*) = tensor.get(indexes.toArray).toFloat

  def data: Array[Float] = tensor.data.asFloat

  /**
   * Utility Functions
   */
  
  def cumsum = tensor.sumNumber.asInstanceOf[Float]

  def dup = new Nd4jMaskedTensor(tensor.dup)

  def isZero = tensor.mul(tensor).sumNumber.asInstanceOf[Float] <= 1E-9
  
  def isZeroShortcut = tensor.sumNumber().asInstanceOf[Float] <= 1E-9

  def max = tensor.maxNumber.asInstanceOf[Float]

  def min = tensor.minNumber.asInstanceOf[Float]

  private implicit def AbstractConvert(array: Nd4jMaskedTensor): Nd4jMaskedTensor = array.asInstanceOf[Nd4jMaskedTensor]
 */
}


object tensorTest extends App {
  var shape = Array(2,2,2)
  val full_mtensor = new Nd4jMaskedTensor( Nd4j.create( Array(1f,2f,3f,4f,5f,6f,7f,8f), shape ), 0 )
  val exec_result = full_mtensor.exec( meanOp, 1 )
  println( "." )
}


