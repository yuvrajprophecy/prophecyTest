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

object EXP_Detect_Type_II {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("SYSDATE").as("LOAD_DTM"),
      col("v_CHANGED_FLAG").as("CHANGED_FLAG"),
      col("SYSDATE").as("NEW_EFFECTIVE_START_DTM"),
      col("CHECKSUM_NBR").as("PREV_CRC_NUM"),
      when(!col("PREV_KEY").isNull.and(
             concat(col("CRC_NUM") === col("PREV_CRC_NUM"), lit(0), lit(1))
           ),
           lit(1)
      ).otherwise(lit(0)).as("v_CHANGED_FLAG"),
      when(col("PREV_KEY").isNull, lit(1))
        .otherwise(col("v_CHANGED_FLAG"))
        .as("NEW_REC"),
      col("VOYAGE_SK").as("PREV_KEY"),
      datetime_add(col("SYSDATE"), lit(0L), lit(0L), lit(0L), lit(-1))
        .as("OLD_EFFECTIVE_END_DTM"),
      lit("Y").as("NEW_CURRENT_FLAG"),
      lit("N").as("OLD_CURRENT_FLAG"),
      col("CRC_NUM"),
      to_date(lit("12/31/2999"), "MM/DD/YYYY").as("NEW_EFFECTIVE_END_DTM")
    )

}
