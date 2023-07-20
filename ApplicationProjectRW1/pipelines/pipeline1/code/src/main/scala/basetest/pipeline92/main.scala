package basetest.pipeline92

import io.prophecy.libs._
import basetest.pipeline92.config.Context
import basetest.pipeline92.config._
import basetest.pipeline92.udfs.UDFs._
import basetest.pipeline92.udfs._
import basetest.pipeline92.udfs.PipelineInitCode._
import basetest.pipeline92.graph._
import basetest.pipeline92.graph.Subgraph_1
import basetest.pipeline92.graph.Subgraph_1.config.{
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
    val df_ds1 = ds1(context)
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1_Context(context.spark, context.config.Subgraph_1),
      df_ds1,
      df_ds1
    )
    val df_ReformatTest_1 = ReformatTest_1(context, df_ds1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline1")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/pipeline1")
    apply(context)
    MetricsCollector.end(spark)
  }

}
