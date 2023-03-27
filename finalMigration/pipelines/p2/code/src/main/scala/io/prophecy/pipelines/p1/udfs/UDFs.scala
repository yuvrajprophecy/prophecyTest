package io.prophecy.pipelines.p1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {
  val x = 1

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("udf1", udf1)
    spark.udf.register("udf2", udf2)
    spark.udf.register("udf3", udf3)
  }

  def udf1 = udf(() => "1")
  def udf2 = udf(() => "1")
  def udf3 = udf(() => "1")
}
