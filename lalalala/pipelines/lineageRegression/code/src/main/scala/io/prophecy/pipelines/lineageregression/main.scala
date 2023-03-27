package io.prophecy.pipelines.lineageregression

import io.prophecy.libs._
import io.prophecy.pipelines.lineageregression.config.Context
import io.prophecy.pipelines.lineageregression.config._
import io.prophecy.pipelines.lineageregression.udfs.UDFs._
import io.prophecy.pipelines.lineageregression.udfs._
import io.prophecy.pipelines.lineageregression.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_normalization     = normalization(context)
    val df_Linear_Regression = Linear_Regression(context, df_normalization)
    val df_Reformat_1        = Reformat_1(context,        df_Linear_Regression)
    val df_Reformat_2        = Reformat_2(context,        df_Reformat_1)
    val df_Reformat_3        = Reformat_3(context,        df_Reformat_2)
    val df_SchemaTransform_1 = SchemaTransform_1(context, df_Linear_Regression)
    normalization_out(context, df_SchemaTransform_1)
    val df_Script_1 = Script_1(context, df_Linear_Regression)
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
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/lineageRegression")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/lineageRegression")
    apply(context)
    MetricsCollector.end(spark)
  }

}
