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
    val df_awkP2G00003RevPivot_FF = awkP2G00003RevPivot_FF(context)
    val df_trParseValues_stage_var =
      trParseValues_stage_var(context, df_awkP2G00003RevPivot_FF)
    val df_trParseValues_V65S6P2_constraint =
      trParseValues_V65S6P2_constraint(context, df_trParseValues_stage_var)
    val df_trParseValues_V65S6P2_reformat = trParseValues_V65S6P2_reformat(
      context,
      df_trParseValues_V65S6P2_constraint
    )
    val (df_swSplitData_lkSessionID,
         df_swSplitData_lkTimeStamp,
         df_swSplitData_lkPageName,
         df_swSplitData_lkPortletName,
         df_swSplitData_lkAction,
         df_swSplitData_lkAccount,
         df_swSplitData_lkCustId,
         df_swSplitData_lkID,
         df_swSplitData_lkStaffNum,
         df_swSplitData_lkChanlId,
         df_swSplitData_lkAppNum,
         df_swSplitData_lkGlblSessnId,
         df_swSplitData_lkJoinNewMIAs
    )                                   = swSplitData(context, df_trParseValues_V65S6P2_reformat)
    val df_awkP2G00003OrigWithRowNum_FF = awkP2G00003OrigWithRowNum_FF(context)
    val df_lkParseRowNum_stage_var =
      lkParseRowNum_stage_var(context, df_awkP2G00003OrigWithRowNum_FF)
    val df_lkParseRowNum_V65S1P4_reformat =
      lkParseRowNum_V65S1P4_reformat(context, df_lkParseRowNum_stage_var)
    val df_mgAttributes_union = mgAttributes_union(
      context,
      df_lkParseRowNum_V65S1P4_reformat,
      df_swSplitData_lkSessionID,
      df_swSplitData_lkTimeStamp,
      df_swSplitData_lkPageName,
      df_swSplitData_lkPortletName,
      df_swSplitData_lkAccount,
      df_swSplitData_lkAction,
      df_swSplitData_lkCustId,
      df_swSplitData_lkID,
      df_swSplitData_lkStaffNum,
      df_swSplitData_lkChanlId,
      df_swSplitData_lkAppNum,
      df_swSplitData_lkGlblSessnId
    )
    val df_mgAttributes_sort = mgAttributes_sort(context, df_mgAttributes_union)
    val df_trHandleNulls_stage_var =
      trHandleNulls_stage_var(context, df_mgAttributes_sort)
    val df_trHandleNulls_V0S833P2_reformat =
      trHandleNulls_V0S833P2_reformat(context, df_trHandleNulls_stage_var)
    val df_fiValidation =
      fiValidation(context, df_trHandleNulls_V0S833P2_reformat)
    val df_joNewMIAs =
      joNewMIAs(context, df_swSplitData_lkJoinNewMIAs, df_fiValidation)
    dsNewMIAs(context,   df_joNewMIAs)
    val df_fiValidation =
      fiValidation(context,    df_trHandleNulls_V0S833P2_reformat)
    sfDOM00001Rejects(context, df_fiValidation)
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
                   "pipelines/pjDominoReadMIAfileMerge"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/pjDominoReadMIAfileMerge") {
      apply(context)
    }
  }

}
