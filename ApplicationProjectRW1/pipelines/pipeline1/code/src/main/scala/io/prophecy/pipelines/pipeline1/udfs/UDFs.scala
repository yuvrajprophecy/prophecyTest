package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("trimUDF",   trimUDF)
    spark.udf.register("square",    square)
    spark.udf.register("square212", square212)
    spark.udf.register("trimUDF1",  trimUDF1)
    spark.udf.register("square1",   square1)
    spark.udf.register("trimUDF2",  trimUDF2)
    spark.udf.register("square3",   square3)
    spark.udf.register("square2",   square2)
    spark.udf.register("square22",  square22)
  }

  def trimUDF = {
    val pipeline2 = "ldme"
    udf((value: String) => value.trim())
  }

  def square = {
    val pipeline2 = "ldme"
    udf((value: Int) => value * value)
  }

  def square212 = {
    val x1 = 11
    udf((value: Int) => value * value)
  }

  def trimUDF1 = {
    val pipeline2 = "ldme"
    udf((value: String) => value.trim())
  }

  def square1 = {
    val pipeline2 = "ldme"
    udf((value: Int) => value * value)
  }

  def trimUDF2 = {
    val pipeline3 = "ldme"
    udf((value: String) => value.trim())
  }

  def square3 = {
    val pipeline3 = "ldme"
    udf((value: Int) => value * value)
  }

  def square2 =
    udf((value2: String) => value2 + value2)

  def square22 = {
    val x = "square22"
    udf((value2: String) => value2 + value2)
  }

}

object PipelineInitCode extends Serializable { val x = 10 }
