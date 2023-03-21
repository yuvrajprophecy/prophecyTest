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

object LKP_V_VOYAGE_RESERVATION_STATUS {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_VOYAGE_RESERVATION_STATUS_Lookup",
             col("VOYAGE_RESERVATION_STATUS_CODE")
      ).getField("VOYAGE_RESERVATION_STATUS_DESC")
        .as("VOYAGE_RESERVATION_STATUS_DESC"),
      lookup("LKP_V_VOYAGE_RESERVATION_STATUS_Lookup",
             col("VOYAGE_RESERVATION_STATUS_CODE")
      ).as("in_VOYAGE_RESERVATION_STATUS_CODE")
    )

}
