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
    val df_TDGetMetaData         = TDGetMetaData(context)
    val df_dsForDetailProcessing = dsForDetailProcessing(context)
    val df_fiPIB                 = fiPIB(context, df_dsForDetailProcessing)
    val df_luMetaData_reject =
      luMetaData_reject(context, df_fiPIB, df_TDGetMetaData)
    val df_soMin          = soMin(context,          df_luMetaData_reject)
    val df_soMax          = soMax(context,          df_luMetaData_reject)
    val df_ddMax          = ddMax(context,          df_soMax)
    val df_ddMin          = ddMin(context,          df_soMin)
    val df_mgMinMax_union = mgMinMax_union(context, df_ddMin, df_ddMax)
    val df_mgMinMax_sort  = mgMinMax_sort(context,  df_mgMinMax_union)
    val df_trAttachDelimiter_stage_var =
      trAttachDelimiter_stage_var(context, df_mgMinMax_sort)
    val df_trAttachDelimiter_V0S994P3_reformat =
      trAttachDelimiter_V0S994P3_reformat(context,
                                          df_trAttachDelimiter_stage_var
      )
    val df_soOutput = soOutput(context, df_trAttachDelimiter_V0S994P3_reformat)
    dsOutputActionRanges(context, df_soOutput)
    val df_soData              = soData(context,              df_fiPIB)
    val df_soChangeKey         = soChangeKey(context,         df_soData)
    val df_trMapCols_stage_var = trMapCols_stage_var(context, df_soChangeKey)
    val df_trMapCols_V156S2P2_reformat =
      trMapCols_V156S2P2_reformat(context, df_trMapCols_stage_var)
    val df_ddRecs     = ddRecs(context,     df_trMapCols_V156S2P2_reformat)
    val df_luMetaData = luMetaData(context, df_fiPIB, df_TDGetMetaData)
    sfOutputData(context, df_ddRecs)
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
                   "pipelines/pjDominoProcessMIActionGeneric_1"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/pjDominoProcessMIActionGeneric_1"
    ) {
      apply(context)
    }
  }

}
