package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("square", square)
    spark.udf.register("trim",   trim)
  }

  def square = {
    val x = 10
    udf((value: Int) => value * value)
  }

  def trim = {
    val y = 10
    udf((value: String) => value.trim())
  }

}

object PipelineInitCode extends Serializable
