package io.prophecy.pipelines.basepipeline2

import io.prophecy.libs._
import io.prophecy.pipelines.basepipeline2.config.Context
import io.prophecy.pipelines.basepipeline2.config._
import io.prophecy.pipelines.basepipeline2.udfs.UDFs._
import io.prophecy.pipelines.basepipeline2.udfs._
import io.prophecy.pipelines.basepipeline2.graph._
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/basePipeline2")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/basePipeline2")
    apply(context)
    MetricsCollector.end(spark)
  }

}
