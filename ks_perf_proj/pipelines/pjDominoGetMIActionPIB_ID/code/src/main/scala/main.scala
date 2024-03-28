import io.prophecy.libs._
import config._
import udfs.UDFs._
import udfs.PipelineInitCode._
import graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_dsNewMIAs = dsNewMIAs(context)
    val df_pscKeyGenerationVC_PIB_ID = pscKeyGenerationVC_PIB_ID.apply(
      pscKeyGenerationVC_PIB_ID.config
        .Context(context.spark, context.config.pscKeyGenerationVC_PIB_ID),
      df_dsNewMIAs
    )
    val df_cpAllPIBs = cpAllPIBs(context, df_pscKeyGenerationVC_PIB_ID)
    val df_soPIBs    = soPIBs(context,    df_cpAllPIBs)
    val df_ddPIBs    = ddPIBs(context,    df_soPIBs)
    dsDistinctPIBs(context, df_ddPIBs)
    val df_cpAllPIBs = cpAllPIBs(context, df_pscKeyGenerationVC_PIB_ID)
    dsPostDominoOut(context, df_cpAllPIBs)
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
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/pjDominoGetMIActionPIB_ID"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/pjDominoGetMIActionPIB_ID") {
      apply(context)
    }
  }

}
