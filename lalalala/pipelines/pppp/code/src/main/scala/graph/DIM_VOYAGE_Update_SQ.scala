package graph

import io.prophecy.libs._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object DIM_VOYAGE_Update_SQ {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("ETL_SESSION_NAME13").as("PROCESS_SESSION_NAME"),
      col("PROCESS_LAST_UPDATE_USER_ID3").as("PROCESS_LAST_UPDATE_USER_ID"),
      col("out_OLD_CURRENT_FLAG").as("CURRENT_FLAG"),
      col("out_OLD_EFFECTIVE_END_DTM").as("EFFECTIVE_END_DATE"),
      col("PREV_KEY").as("VOYAGE_SK"),
      col("PROCESS_LAST_UPDATE_DTM3").as("PROCESS_LAST_UPDATE_DTM")
    )

}
