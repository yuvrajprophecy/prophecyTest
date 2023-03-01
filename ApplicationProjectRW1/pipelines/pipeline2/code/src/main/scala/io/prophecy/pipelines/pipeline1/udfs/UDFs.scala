package io.prophecy.pipelines.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) =
    spark.udf.register("trimUDF", trimUDF)

  def trimUDF = {
    val pipeline1 = "ldme"
    udf((value1: String) => value1.trim())
  }

}

object PipelineInitCode extends Serializable { val pipeline2 = "ldme" }
