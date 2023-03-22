package io.prophecy.pipelines.lineageregression.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("matrix_col_count", matrix_col_count)
    spark.udf.register("generate_matrix",  generate_matrix)
    spark.udf.register("generate_vector",  generate_vector)
    registerAllUDFs(spark)
  }

  def matrix_col_count = {
    import org.apache.spark.ml.linalg.Matrix
    udf((mat: Matrix) => mat.numCols)
  }

  def generate_matrix = {
    import org.apache.spark.ml.linalg.Matrices
    udf(() => Matrices.dense(2, 2, Array(1, 2, 3, 4)))
  }

  def generate_vector = {
    import org.apache.spark.ml.linalg.Vectors
    udf(() => Vectors.dense(1.0d, 2.0d))
  }

}

object PipelineInitCode extends Serializable
