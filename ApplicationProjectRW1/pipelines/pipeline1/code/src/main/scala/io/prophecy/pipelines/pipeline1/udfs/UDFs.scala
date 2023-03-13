package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("trimUDF",        trimUDF)
    spark.udf.register("trimUDF4",       trimUDF4)
    spark.udf.register("createFullName", createFullName)
  }

  def trimUDF = {
    val pipeline1 = "ldme"
    udf((value1: String) => value1.trim())
  }

  def trimUDF4 = {
    val pipeline3 = "ldme"
    udf((value3: String) => value3.trim())
  }

  def createFullName =
    udf((value1: String, value2: String) => value1 + value2)

}

object PipelineInitCode extends Serializable { val pipeline1 = "ldme" }
