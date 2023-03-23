package io.prophecy.pipelines.frfrf

import io.prophecy.libs._
import io.prophecy.pipelines.frfrf.config.Context
import io.prophecy.pipelines.frfrf.config._
import io.prophecy.pipelines.frfrf.udfs.UDFs._
import io.prophecy.pipelines.frfrf.udfs._
import io.prophecy.pipelines.frfrf.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {
  def apply(context: Context): Unit = {}

  def main(args:     Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/frfrf")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/frfrf")
    apply(context)
    MetricsCollector.end(spark)
  }

}
