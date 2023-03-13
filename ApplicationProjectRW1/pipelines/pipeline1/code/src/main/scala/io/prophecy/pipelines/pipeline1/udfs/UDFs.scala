package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("trimUDF2",       trimUDF2)
    spark.udf.register("trimUDF1",       trimUDF1)
    spark.udf.register("createFullName", createFullName)
  }

  def trimUDF2 = {
    val x = 2
    udf((value2: String) => value2.trim())
  }

  def trimUDF1 = {
    val x = 1
    udf((value1: String) => value1.trim())
  }

  def createFullName =
    udf((value1: String, value2: String) => value1 + value2)

}

object PipelineInitCode extends Serializable { val pipeline1 = "ldme" }
