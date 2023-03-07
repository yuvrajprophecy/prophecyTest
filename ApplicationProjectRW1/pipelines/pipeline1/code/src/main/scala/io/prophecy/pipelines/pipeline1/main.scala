package io.prophecy.pipelines.pipeline1

import io.prophecy.libs._
import io.prophecy.pipelines.pipeline1.config.Context
import io.prophecy.pipelines.pipeline1.config._
import io.prophecy.pipelines.pipeline1.udfs.UDFs._
import io.prophecy.pipelines.pipeline1.udfs._
import io.prophecy.pipelines.pipeline1.graph._
import io.prophecy.pipelines.pipeline1.graph.sg1
import io.prophecy.pipelines.pipeline1.graph.sg1.config.{Context => sg1_Context}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_ds1         = ds1(context)
    val df_Reformat_11 = Reformat_11(context, df_ds1)
    val df_sg1 =
      sg1.apply(sg1_Context(context.spark, context.config.sg1), df_Reformat_11)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline1")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/pipeline1")
    apply(context)
    MetricsCollector.end(spark)
  }

}
