package io.prophecy.pipelines.p1

import io.prophecy.libs._
import io.prophecy.pipelines.p1.config.ConfigStore._
import io.prophecy.pipelines.p1.config.Context
import io.prophecy.pipelines.p1.config._
import io.prophecy.pipelines.p1.udfs.UDFs._
import io.prophecy.pipelines.p1.udfs._
import io.prophecy.pipelines.p1.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_de = de(context)
  }

  def main(args: Array[String]): Unit = {
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/p1")
    MetricsCollector.start(spark,                    "pipelines/p1")
    apply(context)
    MetricsCollector.end(spark)
  }

}
