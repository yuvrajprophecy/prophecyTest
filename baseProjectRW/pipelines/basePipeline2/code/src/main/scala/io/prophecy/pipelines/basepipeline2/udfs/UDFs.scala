package io.prophecy.pipelines.basepipeline2.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("createFullName8", createFullName8)
    spark.udf.register("createFullName4", createFullName4)
    spark.udf.register("createFullName6", createFullName6)
    registerAllUDFs(spark)
  }

  def createFullName8 = {
    val x = 8
    udf((v1: String, v2: String) => v1 + v2)
  }

  def createFullName4 = {
    val x = 4
    udf((v1: String, v2: String) => v1 + v2)
  }

  def createFullName6 = {
    val x = 6
    udf((v1: String, v2: String) => v1 + v2)
  }

}

object PipelineInitCode extends Serializable
