package io.prophecy.pipelines.basepipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("createFullName",  createFullName)
    spark.udf.register("createFullName1", createFullName1)
    spark.udf.register("createFullName2", createFullName2)
    spark.udf.register("createFullName3", createFullName3)
    spark.udf.register("createFullName4", createFullName4)
    registerAllUDFs(spark)
  }

  def createFullName = {
    val x = 0
    udf((value: String, value2: String) => value + value2)
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
    val x = 4
    udf((value: String, value2: String) => value + value2)
  }

  def createFullName4 = {
    val x = 5
    udf((value: String, value2: String) => value + value2)
  }

}

object PipelineInitCode extends Serializable
