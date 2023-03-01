package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("trimUDF",        trimUDF)
    spark.udf.register("square",         square)
    spark.udf.register("createFullName", createFullName)
  }

  def trimUDF = {
    val pipeline2 = "ldme"
    udf((value: String) => value.trim())
  }

  def square = {
    val pipeline2 = "ldme"
    udf((value: Int) => value * value)
  }

  def createFullName =
    udf((value1: String, value2: String) => value1 + value2)

}

object PipelineInitCode extends Serializable { val x = 10 }
