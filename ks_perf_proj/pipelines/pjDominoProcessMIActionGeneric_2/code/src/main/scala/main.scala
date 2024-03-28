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
    val df_sfInputData = sfInputData(context)
    val df_trFormatGenericDesc_stage_var =
      trFormatGenericDesc_stage_var(context, df_sfInputData)
    val df_trFormatGenericDesc_V0S1022P2_reformat =
      trFormatGenericDesc_V0S1022P2_reformat(context,
                                             df_trFormatGenericDesc_stage_var
      )
    dsOutputData(context, df_trFormatGenericDesc_V0S1022P2_reformat)
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
                   "pipelines/pjDominoProcessMIActionGeneric_2"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/pjDominoProcessMIActionGeneric_2"
    ) {
      apply(context)
    }
  }

}
