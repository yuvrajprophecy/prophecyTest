package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {
  var x = 10

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("addNumbers10", addNumbers10)
    spark.udf.register("addNumbers11", addNumbers11)
  }

  def addNumbers10 = udf((value: Int, value10: Int) => value + value10)
  def addNumbers11 = udf((value: Int, value11: Int) => value + value11)
}
