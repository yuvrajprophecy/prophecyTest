package io.prophecy.pipelines.application1

import io.prophecy.libs._
import io.prophecy.pipelines.application1.config.ConfigStore._
import io.prophecy.pipelines.application1.config._
import io.prophecy.pipelines.application1.udfs.UDFs._
import io.prophecy.pipelines.application1.udfs._
import io.prophecy.pipelines.application1.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_baseDS1        = baseDS1(spark)
    val df_baseDS3        = baseDS3(spark)
    val df_ApplicationDS1 = ApplicationDS1(spark)
  }

  def main(args: Array[String]): Unit = {
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Application1")
    MetricsCollector.start(
      spark,
      spark.conf.get("prophecy.project.id") + "/" + "pipelines/Application1"
    )
    apply(spark)
    MetricsCollector.end(spark)
  }

}
