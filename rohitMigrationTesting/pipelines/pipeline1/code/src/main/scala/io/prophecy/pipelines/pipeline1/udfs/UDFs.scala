package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {
  var x = 10

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("trimString", trimString)
    spark.udf.register("addNumbers", addNumbers)
  }

  def trimString = udf((value: String) => value.trim())
  def addNumbers = udf((value: Int, value2: Int) => value + value2)
}
