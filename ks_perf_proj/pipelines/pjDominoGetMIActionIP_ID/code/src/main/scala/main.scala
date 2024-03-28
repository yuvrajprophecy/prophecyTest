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
    val df_dsPostDominoOut = dsPostDominoOut(context)
    val df_trAddCduCin_stage_var =
      trAddCduCin_stage_var(context, df_dsPostDominoOut)
    val df_trAddCduCin_V0S927P1_reformat =
      trAddCduCin_V0S927P1_reformat(context, df_trAddCduCin_stage_var)
    val df_P2GTestCustsToExclude_LS = P2GTestCustsToExclude_LS(context)
    val df_ExcludeTestCusts_Lkp_reject = ExcludeTestCusts_Lkp_reject(
      context,
      df_P2GTestCustsToExclude_LS,
      df_trAddCduCin_V0S927P1_reformat
    )
    val df_soMIAs              = soMIAs(context,              df_ExcludeTestCusts_Lkp_reject)
    val df_ddMIAs              = ddMIAs(context,              df_soMIAs)
    val df_trMapKeys_stage_var = trMapKeys_stage_var(context, df_ddMIAs)
    val df_trStafNum_stage_var =
      trStafNum_stage_var(context, df_ExcludeTestCusts_Lkp_reject)
    val df_trStafNum_V0S959P1_constraint =
      trStafNum_V0S959P1_constraint(context, df_trStafNum_stage_var)
    val df_ExcludeTestCusts_Lkp = ExcludeTestCusts_Lkp(
      context,
      df_P2GTestCustsToExclude_LS,
      df_trAddCduCin_V0S927P1_reformat
    )
    val df_trMapKeys_V0S922P1_reformat =
      trMapKeys_V0S922P1_reformat(context, df_trMapKeys_stage_var)
    val df_pscKeyGenerationVC_IP_ID = pscKeyGenerationVC_IP_ID.apply(
      pscKeyGenerationVC_IP_ID.config
        .Context(context.spark, context.config.pscKeyGenerationVC_IP_ID),
      df_trMapKeys_V0S922P1_reformat
    )
    val df_cpMapIP_ID          = cpMapIP_ID(context,          df_pscKeyGenerationVC_IP_ID)
    val df_soRecs              = soRecs(context,              df_cpMapIP_ID)
    val df_soChangeKey         = soChangeKey(context,         df_soRecs)
    val df_trAddRank_stage_var = trAddRank_stage_var(context, df_soChangeKey)
    val df_trAddRank_V0S931P1_reformat =
      trAddRank_V0S931P1_reformat(context, df_trAddRank_stage_var)
    val df_trStafNum_V0S959P1_reformat =
      trStafNum_V0S959P1_reformat(context, df_trStafNum_V0S959P1_constraint)
    val df_soStafNum = soStafNum(context, df_trStafNum_V0S959P1_reformat)
    val df_ddStafNum = ddStafNum(context, df_soStafNum)
    val df_pscKeyGenerationVC_IP_ID_EMPLY =
      pscKeyGenerationVC_IP_ID_EMPLY.apply(
        pscKeyGenerationVC_IP_ID_EMPLY.config.Context(
          context.spark,
          context.config.pscKeyGenerationVC_IP_ID_EMPLY
        ),
        df_ddStafNum
      )
    val df_cpMapIP_ID_EMPLY =
      cpMapIP_ID_EMPLY(context, df_pscKeyGenerationVC_IP_ID_EMPLY)
    val df_soLkpStafNum = soLkpStafNum(context, df_cpMapIP_ID_EMPLY)
    val df_soDataStafNum =
      soDataStafNum(context, df_trAddRank_V0S931P1_reformat)
    val df_joStafNum = joStafNum(context, df_soDataStafNum, df_soLkpStafNum)
    dsIP_IDs(context, df_joStafNum)
    val df_joIPnRank = joIPnRank(context,
                                 df_ExcludeTestCusts_Lkp_reject,
                                 df_trAddRank_V0S931P1_reformat
    )
    dsForDetailProcessing(context, df_joIPnRank)
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
                   "pipelines/pjDominoGetMIActionIP_ID"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/pjDominoGetMIActionIP_ID") {
      apply(context)
    }
  }

}
