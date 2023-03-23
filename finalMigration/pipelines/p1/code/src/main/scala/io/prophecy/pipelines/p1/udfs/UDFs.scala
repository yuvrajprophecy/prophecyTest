package io.prophecy.pipelines.p1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("udf11", udf11)
    spark.udf.register("udf22", udf22)
    registerAllUDFs(spark)
  }

  def udf11 = {
    val x = 3
    udf(() => "3")
  }

  def udf22 = {
    val x = 3
    udf(() => "3")
  }

}

object PipelineInitCode extends Serializable { val x = 1 }
