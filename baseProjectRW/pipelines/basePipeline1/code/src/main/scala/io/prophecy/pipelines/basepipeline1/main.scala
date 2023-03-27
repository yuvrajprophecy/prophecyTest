package io.prophecy.pipelines.basepipeline1

import io.prophecy.libs._
import io.prophecy.pipelines.basepipeline1.config.Context
import io.prophecy.pipelines.basepipeline1.config._
import io.prophecy.pipelines.basepipeline1.udfs.UDFs._
import io.prophecy.pipelines.basepipeline1.udfs._
import io.prophecy.pipelines.basepipeline1.graph._
import io.prophecy.pipelines.basepipeline1.graph.Subgraph_1
import io.prophecy.pipelines.basepipeline1.graph.Subgraph_2
import io.prophecy.pipelines.basepipeline1.graph.Subgraph_1.config.{
  Context => Subgraph_1_Context
}
import io.prophecy.pipelines.basepipeline1.graph.Subgraph_2.config.{
  Context => Subgraph_2_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_baseDS1    = baseDS1(context)
    val df_Reformat_2 = Reformat_2(context, df_baseDS1)
    val df_Reformat_3 = Reformat_3(context, df_Reformat_2)
    val df_Join_1     = Join_1(context,     df_Reformat_2, df_Reformat_2)
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1_Context(context.spark, context.config.Subgraph_1),
      df_Reformat_3
    )
    baseDS2(context, df_Subgraph_1)
    val df_Subgraph_2 = Subgraph_2.apply(
      Subgraph_2_Context(context.spark, context.config.Subgraph_2),
      df_Join_1
    )
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/basePipeline1")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/basePipeline1")
    apply(context)
    MetricsCollector.end(spark)
  }

}
