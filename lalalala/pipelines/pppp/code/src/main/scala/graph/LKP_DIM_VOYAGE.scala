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

object LKP_DIM_VOYAGE {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_DIM_VOYAGE_Lookup", col("SHIP_DEPARTURE_DATE"))
        .as("in_SHIP_DEPARTURE_DATE"),
      lookup("LKP_DIM_VOYAGE_Lookup", col("SHIP_CODE"))
        .getField("OBS_RECEIVED_DTM")
        .as("OBS_RECEIVED_DTM"),
      lookup("LKP_DIM_VOYAGE_Lookup", col("SHIP_CODE"))
        .getField("OBS_RECEIVED_FLAG")
        .as("OBS_RECEIVED_FLAG"),
      lookup("LKP_DIM_VOYAGE_Lookup", col("SHIP_CODE"))
        .getField("VOYAGE_SK")
        .as("VOYAGE_SK"),
      lookup("LKP_DIM_VOYAGE_Lookup", col("SHIP_CODE")).as("in_SHIP_CODE"),
      lookup("LKP_DIM_VOYAGE_Lookup", col("SHIP_CODE"))
        .getField("CHECKSUM_NBR")
        .as("CHECKSUM_NBR")
    )

}
