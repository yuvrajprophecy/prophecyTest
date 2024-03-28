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
    val df_TDGenericTableUnload = TDGenericTableUnload(context)
    val df_moFieldNamesLkp      = moFieldNamesLkp(context, df_TDGenericTableUnload)
    val df_dsInputData          = dsInputData(context)
    val df_moFieldNamesIn       = moFieldNamesIn(context,  df_dsInputData)
    val df_luRefExists =
      luRefExists(context, df_moFieldNamesLkp, df_moFieldNamesIn)
    val df_luRefExists_reject =
      luRefExists_reject(context, df_moFieldNamesLkp, df_moFieldNamesIn)
    val df_moFieldNamesOut = moFieldNamesOut(context, df_luRefExists_reject)
    dsLoadData(context, df_moFieldNamesOut)
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
                   "pipelines/pjGenericRestartableUnload_4Keys"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/pjGenericRestartableUnload_4Keys"
    ) {
      apply(context)
    }
  }

}
