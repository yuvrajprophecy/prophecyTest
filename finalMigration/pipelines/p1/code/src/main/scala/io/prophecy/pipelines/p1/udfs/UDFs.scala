package io.prophecy.pipelines.p1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("udf71", udf71)
    registerAllUDFs(spark)
  }

  def udf71 =
    udf(() => "7")

}

object PipelineInitCode extends Serializable { val x = 1 }
