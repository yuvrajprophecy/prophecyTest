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
    val df_dsIP_IDs = dsIP_IDs(context)
    val df_trHandleNulls_stage_var =
      trHandleNulls_stage_var(context, df_dsIP_IDs)
    val df_trHandleNulls_V0S929P3_constraint =
      trHandleNulls_V0S929P3_constraint(context, df_trHandleNulls_stage_var)
    val df_trHandleNulls_V0S929P3_reformat = trHandleNulls_V0S929P3_reformat(
      context,
      df_trHandleNulls_V0S929P3_constraint
    )
    val df_soAppId = soAppId(context, df_trHandleNulls_V0S929P3_reformat)
    val df_ddAppId = ddAppId(context, df_soAppId)
    val df_trHandleNulls_V0S929P2_reformat =
      trHandleNulls_V0S929P2_reformat(context, df_trHandleNulls_stage_var)
    val df_trAdjTimeNcheckAcct_stage_var =
      trAdjTimeNcheckAcct_stage_var(context, df_trHandleNulls_V0S929P2_reformat)
    val df_trAdjTimeNcheckAcct_V0S919P2_constraint =
      trAdjTimeNcheckAcct_V0S919P2_constraint(context,
                                              df_trAdjTimeNcheckAcct_stage_var
      )
    val df_trAdjTimeNcheckAcct_V0S919P2_reformat =
      trAdjTimeNcheckAcct_V0S919P2_reformat(
        context,
        df_trAdjTimeNcheckAcct_V0S919P2_constraint
      )
    val df_trAdjTimeNcheckAcct_V0S919P3_constraint =
      trAdjTimeNcheckAcct_V0S919P3_constraint(context,
                                              df_trAdjTimeNcheckAcct_stage_var
      )
    val df_trAdjTimeNcheckAcct_V0S919P3_reformat =
      trAdjTimeNcheckAcct_V0S919P3_reformat(
        context,
        df_trAdjTimeNcheckAcct_V0S919P3_constraint
      )
    val df_pscKeyGenerationVC_ARRG_ID = pscKeyGenerationVC_ARRG_ID.apply(
      pscKeyGenerationVC_ARRG_ID.config
        .Context(context.spark, context.config.pscKeyGenerationVC_ARRG_ID),
      df_trAdjTimeNcheckAcct_V0S919P2_reformat
    )
    val df_cpMapCols = cpMapCols(context, df_pscKeyGenerationVC_ARRG_ID)
    val df_fuCollectRecs = fuCollectRecs(
      context,
      df_trAdjTimeNcheckAcct_V0S919P3_reformat,
      df_cpMapCols
    )
    val df_trMapCols_stage_var = trMapCols_stage_var(context, df_fuCollectRecs)
    val df_trMapCols_V78S0P7_reformat =
      trMapCols_V78S0P7_reformat(context, df_trMapCols_stage_var)
    val df_pscKeyGenerationVC_ARRG_ID_APP =
      pscKeyGenerationVC_ARRG_ID_APP.apply(
        pscKeyGenerationVC_ARRG_ID_APP.config.Context(
          context.spark,
          context.config.pscKeyGenerationVC_ARRG_ID_APP
        ),
        df_ddAppId
      )
    val df_cpArrgIdApp = cpArrgIdApp(context, df_pscKeyGenerationVC_ARRG_ID_APP)
    val df_joArrgIdApp =
      joArrgIdApp(context,    df_trMapCols_V78S0P7_reformat, df_cpArrgIdApp)
    dsLoadDwhPibActn(context, df_joArrgIdApp)
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
                   "pipelines/pjDominoGetMIActionARRG_ID"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/pjDominoGetMIActionARRG_ID") {
      apply(context)
    }
  }

}
