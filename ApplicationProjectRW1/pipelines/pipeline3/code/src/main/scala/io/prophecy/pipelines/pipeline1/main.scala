package io.prophecy.pipelines.pipeline1

import io.prophecy.libs._
import io.prophecy.pipelines.pipeline1.config.ConfigStore._
import io.prophecy.pipelines.pipeline1.config.Context
import io.prophecy.pipelines.pipeline1.config._
import io.prophecy.pipelines.pipeline1.udfs.UDFs._
import io.prophecy.pipelines.pipeline1.udfs._
import io.prophecy.pipelines.pipeline1.graph._
import io.prophecy.pipelines.pipeline1.graph.Subgraph_1
import io.prophecy.pipelines.pipeline1.graph.basesg1_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_ds1        = ds1(context)
    val df_Reformat_1 = Reformat_1(context,       df_ds1)
    val df_Subgraph_1 = Subgraph_1.apply(context, df_Reformat_1)
    val df_basesg1_1  = basesg1_1.apply(context,  df_Reformat_1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline3")
    MetricsCollector.start(spark,                    "pipelines/pipeline3")
    apply(context)
    MetricsCollector.end(spark)
  }

}
