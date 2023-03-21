package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LKP_V_ITINERARY_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_V_ITINERARY_Lookup",
      in,
      context.spark,
      List("COUNT_REPORT_ITINERARY_CODE1", "SHIP_DEPARTURE_DATE"),
      "ITINERARY_VOYAGE_EFF_FROM_DATE",
      "COUNT_REPORT_ITINERARY_CODE",
      "ITINERARY_DESC"
    )

}
