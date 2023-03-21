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

object LKP_V_REGION_TRAVEL_AREA {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_REGION_TRAVEL_AREA_Lookup", col("REGION_TRAVEL_AREA_CODE"))
        .as("in_REGION_TRAVEL_AREA_CODE"),
      lookup("LKP_V_REGION_TRAVEL_AREA_Lookup", col("REGION_TRAVEL_AREA_CODE"))
        .getField("REGION_TRAVEL_AREA_DESC")
        .as("REGION_TRAVEL_AREA_DESC")
    )

}
