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
    val df_dsPreviousLoopData    = dsPreviousLoopData(context)
    val df_dsForDetailProcessing = dsForDetailProcessing(context)
    val df_fiAttribute           = fiAttribute(context,  df_dsForDetailProcessing)
    val df_moFieldNames          = moFieldNames(context, df_fiAttribute)
    val df_mgData_union =
      mgData_union(context, df_dsPreviousLoopData, df_moFieldNames)
    val df_mgData_sort = mgData_sort(context, df_mgData_union)
    dsOutputData(context, df_mgData_sort)
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
                   "pipelines/pjDominoProcessMIActionElement_N"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/pjDominoProcessMIActionElement_N"
    ) {
      apply(context)
    }
  }

}
