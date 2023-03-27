package simpledatalabs_27.sfefef.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Vector_to_array extends Serializable {
  import org.apache.spark.mllib.linalg.Vector
  def vector_to_array = udf((value: Vector) => value.toArray)
}
