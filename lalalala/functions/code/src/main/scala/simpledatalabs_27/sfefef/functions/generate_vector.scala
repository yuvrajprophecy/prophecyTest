package simpledatalabs_27.sfefef.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Generate_vector extends Serializable {
  import org.apache.spark.ml.linalg.Vectors
  def generate_vector = udf(() => Vectors.dense(0.1d, 0.2d, 0.3d))
}
