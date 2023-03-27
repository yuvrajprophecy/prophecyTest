package simpledatalabs_27.sfefef.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Generate_matrix extends Serializable {
  import org.apache.spark.ml.linalg.{Matrices, Matrix}

  def generate_matrix =
    udf { () =>
      Matrices.dense(3, 2, Array(1.0d, 3.0d, 5.0d, 2.0d, 4.0d, 6.0d))
    }

}
