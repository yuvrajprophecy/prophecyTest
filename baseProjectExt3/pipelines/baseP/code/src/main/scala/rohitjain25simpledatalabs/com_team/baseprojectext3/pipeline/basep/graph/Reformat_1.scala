package rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.graph

import io.prophecy.libs._
import rohitjain25simpledatalabs.com_team.baseprojectext3.functions._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(sharedSquare(col("id")).as("id"))

}
