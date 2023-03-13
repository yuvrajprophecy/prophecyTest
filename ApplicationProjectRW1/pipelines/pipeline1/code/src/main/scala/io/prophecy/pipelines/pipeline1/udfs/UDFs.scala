package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("trimUDF",        trimUDF)
    spark.udf.register("trimUDF2",       trimUDF2)
    spark.udf.register("trimUDF3",       trimUDF3)
    spark.udf.register("createFullName", createFullName)
  }

  def trimUDF = {
    val x = 0
    udf((value: String) => value.trim())
  }

  def trimUDF2 = {
    val x = 2
    udf((value2: String) => value2.trim())
  }

  def trimUDF3 = {
    val x = 3
    udf((value3: String) => value3.trim())
  }

  def createFullName =
    udf((value1: String, value2: String) => value1 + value2)

}

object PipelineInitCode extends Serializable { val pipeline1 = "ldme" }
