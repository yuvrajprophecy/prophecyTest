package io.prophecy.pipelines.p1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("udf12", udf12)
    spark.udf.register("udf21", udf21)
    spark.udf.register("udf32", udf32)
    registerAllUDFs(spark)
  }

  def udf12 = {
    val x = 3
    udf(() => "3")
  }

  def udf21 = {
    val x = 3
    udf(() => "3")
  }

  def udf32 = {
    val x = 3
    udf(() => "3")
  }

}

object PipelineInitCode extends Serializable { val x = 1 }
