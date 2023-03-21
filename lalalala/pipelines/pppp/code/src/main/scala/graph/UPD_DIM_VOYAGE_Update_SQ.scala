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

object UPD_DIM_VOYAGE_Update_SQ {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("PROCESS_LAST_UPDATE_DTM").as("PROCESS_LAST_UPDATE_DTM3"),
      col("ETL_SESSION_NAME1").as("ETL_SESSION_NAME13"),
      col("PREV_KEY").as("PREV_KEY3"),
      col("OLD_EFFECTIVE_END_DTM").as("OLD_EFFECTIVE_END_DTM3"),
      col("PROCESS_LAST_UPDATE_USER_ID").as("PROCESS_LAST_UPDATE_USER_ID3"),
      col("OLD_CURRENT_FLAG").as("OLD_CURRENT_FLAG3")
    )

}
