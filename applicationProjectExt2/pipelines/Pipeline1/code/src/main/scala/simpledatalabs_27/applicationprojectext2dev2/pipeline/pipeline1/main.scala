package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1

import io.prophecy.libs._
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config.Context
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config._
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.udfs.UDFs._
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.udfs._
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_d1         = d1(context)
    val df_Reformat_1 = Reformat_1(context, df_d1)
    val df_Subgraph_1 =
      simpledatalabs_27.applicationprojectext2dev2.subgraph.sg1.apply(
        simpledatalabs_27.applicationprojectext2dev2.subgraph.sg1.config
          .Context(context.spark, context.config.Subgraph_1),
        df_Reformat_1
      )
    d2(context, df_Subgraph_1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Pipeline1")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/Pipeline1")
    apply(context)
    MetricsCollector.end(spark)
  }

}
