package basetest.pipeline9

import io.prophecy.libs._
import basetest.pipeline9.config.Context
import basetest.pipeline9.config._
import basetest.pipeline9.config.ConfigStore.interimOutput
import basetest.pipeline9.udfs.UDFs._
import basetest.pipeline9.udfs._
import basetest.pipeline9.udfs.PipelineInitCode._
import basetest.pipeline9.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    val df_ds1 = ds1(context).interim(
      "graph",
      "RYQnrtN3ocY6_boAf7-HX$$2kX7OLlbNvV4-07_cW-kx",
      "7HgZqmV9hcpmDeE3tJeXC$$f_L2YxzqZoneVuRsUJHdF"
    )
    val df_Reformat_1 = Reformat_1(context, df_ds1).interim(
      "graph",
      "JyDMsNrosYkFO0fDZFN_e$$JZFXLetzwrJcBYNfJAtU4",
      "KzTT7eEaOo_qqVQ0jYPBG$$rCMVi17g-CG1I1neWJJd1"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()
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
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules",
                   "org.apache.spark.sql.catalyst.optimizer.ColumnPruning"
    )
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/kaajri12312")
    try MetricsCollector.start(spark,                "pipelines/kaajri12312", context.config)
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/kaajri12312")
    }
    graph(context)
    MetricsCollector.end(spark)
  }

}
