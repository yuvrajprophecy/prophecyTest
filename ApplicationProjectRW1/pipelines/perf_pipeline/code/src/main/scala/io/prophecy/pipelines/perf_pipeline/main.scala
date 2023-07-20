package io.prophecy.pipelines.perf_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.perf_pipeline.config.Context
import io.prophecy.pipelines.perf_pipeline.config._
import io.prophecy.pipelines.perf_pipeline.udfs.UDFs._
import io.prophecy.pipelines.perf_pipeline.udfs._
import io.prophecy.pipelines.perf_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.perf_pipeline.graph._
import io.prophecy.pipelines.perf_pipeline.graph.Subgraph_1
import io.prophecy.pipelines.perf_pipeline.graph.Subgraph_1.config.{
  Context => Subgraph_1_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_largeDataset1     = largeDataset1(context)
    val df_Reformat_1        = Reformat_1(context, df_largeDataset1)
    val df_OrderBy_1         = OrderBy_1(context,  df_Reformat_1)
    val df_largeDataset1_1_1 = largeDataset1_1_1(context)
    val df_largeDataset1_1   = largeDataset1_1(context)
    val df_largeDataset1_1_2 = largeDataset1_1_2(context)
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1_Context(context.spark, context.config.Subgraph_1),
      df_largeDataset1_1_2
    )
    val df_Filter_2 = Filter_2(context, df_OrderBy_1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/perf_pipeline")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/perf_pipeline")
    apply(context)
    MetricsCollector.end(spark)
  }

}
