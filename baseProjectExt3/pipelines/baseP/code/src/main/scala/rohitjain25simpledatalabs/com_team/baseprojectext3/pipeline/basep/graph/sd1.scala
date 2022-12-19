package rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.graph

import io.prophecy.libs._
import rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sd1 {

  def apply(spark: SparkSession): DataFrame =
    spark.read
      .format("csv")
      .option("header",                          true)
      .option("sep",                             ",")
      .schema(StructType(Array(StructField("id", StringType, true))))
      .load("dede")

}
