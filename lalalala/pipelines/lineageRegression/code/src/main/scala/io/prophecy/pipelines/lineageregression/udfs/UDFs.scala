package io.prophecy.pipelines.lineageregression.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("vector_to_array",  vector_to_array)
    spark.udf.register("generate_vector",  generate_vector)
    spark.udf.register("generate_matrix",  generate_matrix)
    spark.udf.register("matrix_col_count", matrix_col_count)
    registerAllUDFs(spark)
  }

  def vector_to_array = {
    import org.apache.spark.mllib.linalg.Vector
    udf((value: Vector) => value.toArray)
  }

  def generate_vector = {
    import org.apache.spark.ml.linalg.Vectors
    udf(() => Vectors.dense(0.1d, 0.2d, 0.3d))
  }

  def generate_matrix = {
    import org.apache.spark.ml.linalg.{Matrices, Matrix}
    udf { () =>
      Matrices.dense(3, 2, Array(1.0d, 3.0d, 5.0d, 2.0d, 4.0d, 6.0d))
    }
  }

  def matrix_col_count = {
    import org.apache.spark.ml.linalg.{Matrices, Matrix}
    udf((mat: Matrix) => mat.numCols)
  }

}

object PipelineInitCode extends Serializable
