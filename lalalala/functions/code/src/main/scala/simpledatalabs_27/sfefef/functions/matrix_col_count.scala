package simpledatalabs_27.sfefef.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Matrix_col_count extends Serializable {
  import org.apache.spark.ml.linalg.{Matrices, Matrix}
  def matrix_col_count = udf((mat: Matrix) => mat.numCols)
}
