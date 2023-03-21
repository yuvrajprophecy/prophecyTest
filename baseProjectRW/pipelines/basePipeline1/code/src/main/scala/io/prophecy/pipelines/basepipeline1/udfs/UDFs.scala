package io.prophecy.pipelines.basepipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("createFullName", createFullName)
    registerAllUDFs(spark)
  }

  def createFullName = {
    val x = 0
    udf((value: String, value2: String) => value + value2)
  }

}

object PipelineInitCode extends Serializable
