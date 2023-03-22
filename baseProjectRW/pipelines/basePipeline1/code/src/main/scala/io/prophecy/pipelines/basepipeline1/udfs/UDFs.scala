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
    registerAllUDFs(spark)
  }

  def createFullName1 = {
    val x = 1
    udf((v1: String, v2: String) => v1 + v2)
  }

  def createFullName2 = {
    val x = 2
    udf((v1: String, v2: String) => v1 + v2)
  }

  def createFullName3 = {
    val x = 3
    udf((v1: String, v2: String) => v1 + v2)
  }

  def createFullName4 = {
    val x = 4
    udf((v1: String, v2: String) => v1 + v2)
  }

}

object PipelineInitCode extends Serializable
