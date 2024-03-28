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
    val df_dsForDetailProcessing = dsForDetailProcessing(context)
    val df_fiAttribute           = fiAttribute(context, df_dsForDetailProcessing)
    val df_soData                = soData(context,      df_fiAttribute)
    val df_ddData                = ddData(context,      df_soData)
    dsPibSubsetForProcessing(context, df_fiAttribute)
    val df_trAddRecAcdt_stage_var = trAddRecAcdt_stage_var(context, df_ddData)
    val df_trAddRecAcdt_V0S921P1_reformat =
      trAddRecAcdt_V0S921P1_reformat(context, df_trAddRecAcdt_stage_var)
    dsOutputData(context,                     df_trAddRecAcdt_V0S921P1_reformat)
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
                   "pipelines/pjDominoProcessMIActionElement_1"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/pjDominoProcessMIActionElement_1"
    ) {
      apply(context)
    }
  }

}
