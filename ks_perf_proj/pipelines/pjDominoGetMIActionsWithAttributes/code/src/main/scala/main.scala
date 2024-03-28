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
    val df_dsDataIn         = dsDataIn(context)
    val df_soData           = soData(context,           df_dsDataIn)
    val df_ddSampleData     = ddSampleData(context,     df_soData)
    val df_luSampleRec      = luSampleRec(context,      df_dsDataIn, df_ddSampleData)
    val df_soAttributes     = soAttributes(context,     df_luSampleRec)
    val df_ddRemovDupPibs   = ddRemovDupPibs(context,   df_soAttributes)
    val df_fiNullAttributes = fiNullAttributes(context, df_ddRemovDupPibs)
    val df_esGetMiaMetadata = esGetMiaMetadata(context)
    val df_luCheckExists =
      luCheckExists(context, df_esGetMiaMetadata, df_fiNullAttributes)
    val df_trTrim_stage_var = trTrim_stage_var(context, df_luCheckExists)
    val df_trTrim_V0S955P2_reformat =
      trTrim_V0S955P2_reformat(context, df_trTrim_stage_var)
    val df_luCheckCorrect_reject = luCheckCorrect_reject(
      context,
      df_soAttributes,
      df_trTrim_V0S955P2_reformat
    )
    val df_soRejects = soRejects(context, df_luCheckCorrect_reject)
    sfRejects(context, df_soRejects)
    val df_luCheckCorrect =
      luCheckCorrect(context, df_soAttributes, df_trTrim_V0S955P2_reformat)
    val df_fiNullAttributes = fiNullAttributes(context, df_ddRemovDupPibs)
    dsPIBsWithAttributes(context, df_fiNullAttributes)
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
                   "pipelines/pjDominoGetMIActionsWithAttributes"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/pjDominoGetMIActionsWithAttributes"
    ) {
      apply(context)
    }
  }

}
