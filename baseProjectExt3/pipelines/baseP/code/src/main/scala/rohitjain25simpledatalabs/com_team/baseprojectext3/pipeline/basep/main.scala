package rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep

import io.prophecy.libs._
import rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.config.ConfigStore._
import rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.config._
import rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.udfs.UDFs._
import rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.udfs._
import rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_sd1        = sd1(spark)
    val df_Reformat_1 = Reformat_1(spark, df_sd1)
    val df_Subgraph_1 =
      rohitjain25simpledatalabs.com_team.baseprojectext3.subgraph.sharedsg1
        .apply(spark, df_Reformat_1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/baseP")
    registerUDFs(spark)
    MetricsCollector.start(
      spark,
      spark.conf.get("prophecy.project.id") + "/" + "pipelines/baseP"
    )
    apply(spark)
    MetricsCollector.end(spark)
  }

}
