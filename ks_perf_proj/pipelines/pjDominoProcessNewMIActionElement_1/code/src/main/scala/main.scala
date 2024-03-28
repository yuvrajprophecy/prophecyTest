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
    val df_TDGetMetaData      = TDGetMetaData(context)
    val df_ciParseGenericItem = ciParseGenericItem(context, df_TDGetMetaData)
    dsOutputData(context, df_ciParseGenericItem)
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
                   "pipelines/pjDominoProcessNewMIActionElement_1"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/pjDominoProcessNewMIActionElement_1"
    ) {
      apply(context)
    }
  }

}
