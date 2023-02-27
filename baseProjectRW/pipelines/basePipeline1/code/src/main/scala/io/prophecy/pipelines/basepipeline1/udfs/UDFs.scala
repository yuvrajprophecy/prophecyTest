package io.prophecy.pipelines.basepipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("square2",  square2)
    spark.udf.register("square22", square22)
  }

  def square2 =
    udf((value2: String) => value2 + value2)

  def square22 = {
    val x = "square22"
    udf((value2: String) => value2 + value2)
  }

}

object PipelineInitCode extends Serializable
