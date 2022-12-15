package simpledatalabs_27.applicationprojectext2.pipeline.pipeline1

import io.prophecy.libs._
import simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.config.ConfigStore._
import simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.config._
import simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.udfs.UDFs._
import simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.udfs._
import simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.graph._
import simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.graph.Subgraph_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_d1         = d1(spark)
    val df_Reformat_1 = Reformat_1(spark, df_d1)
    val df_sharedSG1_1 =
      rohitjain25simpledatalabs.com_team.baseprojectext3.subgraph.sharedsg1
        .apply(spark, df_Reformat_1)
    val df_Subgraph_1 = Subgraph_1.apply(spark, df_Reformat_1)
    d2(spark, df_Subgraph_1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Pipeline1")
    registerUDFs(spark)
    MetricsCollector.start(
      spark,
      spark.conf.get("prophecy.project.id") + "/" + "pipelines/Pipeline1"
    )
    apply(spark)
    MetricsCollector.end(spark)
  }

}
