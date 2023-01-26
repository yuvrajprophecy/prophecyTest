package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {
  var x = 10

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("trimString",   trimString)
    spark.udf.register("addNumbers",   addNumbers)
    spark.udf.register("addNumbers1",  addNumbers1)
    spark.udf.register("addNumbers2",  addNumbers2)
    spark.udf.register("addNumbers3",  addNumbers3)
    spark.udf.register("addNumbers4",  addNumbers4)
    spark.udf.register("addNumbers5",  addNumbers5)
    spark.udf.register("addNumbers6",  addNumbers6)
    spark.udf.register("addNumbers7",  addNumbers7)
    spark.udf.register("addNumbers8",  addNumbers8)
    spark.udf.register("addNumbers9",  addNumbers9)
    spark.udf.register("addNumbers10", addNumbers10)
    spark.udf.register("addNumbers11", addNumbers11)
  }

  def trimString   = udf((value: String) => value.trim())
  def addNumbers   = udf((value: Int, value2: Int) => value + value2)
  def addNumbers1  = udf((value: Int, value1: Int) => value + value1)
  def addNumbers2  = udf((value: Int, value2: Int) => value + value2)
  def addNumbers3  = udf((value: Int, value3: Int) => value + value3)
  def addNumbers4  = udf((value: Int, value4: Int) => value + value4)
  def addNumbers5  = udf((value: Int, value5: Int) => value + value5)
  def addNumbers6  = udf((value: Int, value6: Int) => value + value6)
  def addNumbers7  = udf((value: Int, value7: Int) => value + value7)
  def addNumbers8  = udf((value: Int, value8: Int) => value + value8)
  def addNumbers9  = udf((value: Int, value9: Int) => value + value9)
  def addNumbers10 = udf((value: Int, value10: Int) => value + value10)
  def addNumbers11 = udf((value: Int, value11: Int) => value + value11)
}
