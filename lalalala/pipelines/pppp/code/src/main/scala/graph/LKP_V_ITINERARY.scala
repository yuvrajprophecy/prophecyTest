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

object LKP_V_ITINERARY {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_ITINERARY_Lookup", col("SHIP_DEPARTURE_DATE"))
        .as("SHIP_DEPARTURE_DATE"),
      lookup("LKP_V_ITINERARY_Lookup", col("COUNT_REPORT_ITINERARY_CODE"))
        .as("COUNT_REPORT_ITINERARY_CODE1"),
      lookup("LKP_V_ITINERARY_Lookup", col("SHIP_DEPARTURE_DATE"))
        .getField("ITINERARY_DESC")
        .as("ITINERARY_DESC")
    )

}
