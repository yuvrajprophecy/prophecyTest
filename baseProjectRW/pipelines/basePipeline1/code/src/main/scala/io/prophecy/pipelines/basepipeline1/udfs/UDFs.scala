package io.prophecy.pipelines.basepipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("createFullName1", createFullName1)
    spark.udf.register("createFullName2", createFullName2)
    spark.udf.register("createFullName3", createFullName3)
    spark.udf.register("createFullName4", createFullName4)
    spark.udf.register("createFullName5", createFullName5)
    spark.udf.register("createFullName6", createFullName6)
    registerAllUDFs(spark)
  }

  def createFullName1 = {
    val x = 1
    udf((value: String, value2: String) => value + value2)
  }

  def createFullName2 = {
    val x = 2
    udf((value: String, value2: String) => value + value2)
  }

  def createFullName3 = {
    val x = 3
    udf((value: String, value2: String) => value + value2)
  }

  def createFullName4 = {
    val x = 4
    udf((value: String, value2: String) => value + value2)
  }

  def createFullName5 = {
    val x = 5
    udf((value: String, value2: String) => value + value2)
  }

  def createFullName6 = {
    val x = 6
    udf((value: String, value2: String) => value + value2)
  }

}

object PipelineInitCode extends Serializable
