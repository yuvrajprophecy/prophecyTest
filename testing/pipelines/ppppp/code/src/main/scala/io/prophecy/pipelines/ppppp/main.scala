package io.prophecy.pipelines.ppppp

import io.prophecy.libs._
import io.prophecy.pipelines.ppppp.config.ConfigStore._
import io.prophecy.pipelines.ppppp.config.Context
import io.prophecy.pipelines.ppppp.config._
import io.prophecy.pipelines.ppppp.udfs.UDFs._
import io.prophecy.pipelines.ppppp.udfs._
import io.prophecy.pipelines.ppppp.graph._
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/ppppp")
    MetricsCollector.start(spark,                    "pipelines/ppppp")
    apply(context)
    MetricsCollector.end(spark)
  }

}
