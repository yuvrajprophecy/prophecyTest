package io.prophecy.pipelines.deed

import io.prophecy.libs._
import io.prophecy.pipelines.deed.config.Context
import io.prophecy.pipelines.deed.config._
import io.prophecy.pipelines.deed.udfs.UDFs._
import io.prophecy.pipelines.deed.udfs._
import io.prophecy.pipelines.deed.graph._
import io.prophecy.pipelines.deed.graph.Subgraph_1
import io.prophecy.pipelines.deed.graph.Subgraph_1.config.{
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
    val df_normalization = normalization(context)
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1_Context(context.spark, context.config.Subgraph_1),
      df_normalization
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/deed")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/deed")
    apply(context)
    MetricsCollector.end(spark)
  }

}
