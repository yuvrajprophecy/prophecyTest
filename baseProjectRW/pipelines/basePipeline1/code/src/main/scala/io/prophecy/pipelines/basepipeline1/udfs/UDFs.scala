package io.prophecy.pipelines.basepipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("createFullName2", createFullName2)
    spark.udf.register("createFullName1", createFullName1)
    spark.udf.register("createFullName",  createFullName)
    registerAllUDFs(spark)
  }

  def createFullName2 = {
    val x = 3
    udf((value1: String, value2: String) => value1 + value2)
  }

  def createFullName1 = {
    val x = 4
    udf((value1: String, value2: String) => value1 + value2)
  }

  def createFullName = {
    val x = 5
    udf((value1: String, value2: String) => value1 + value2)
  }

}

object PipelineInitCode extends Serializable
