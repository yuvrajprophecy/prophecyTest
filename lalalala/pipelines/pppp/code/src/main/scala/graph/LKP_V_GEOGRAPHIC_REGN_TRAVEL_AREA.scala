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

object LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup",
             col("GEOGRAPHIC_REGION_CODE")
      ).as("in_GEOGRAPHIC_REGION_CODE"),
      lookup("LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup", col("BRAND_CODE"))
        .getField("REGN_TRAVL_AREA_SAIL_PERD_TEXT")
        .as("REGN_TRAVL_AREA_SAIL_PERD_TEXT"),
      lookup("LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup", col("BRAND_CODE"))
        .getField("REGION_TRAVEL_AREA_TEXT")
        .as("REGION_TRAVEL_AREA_TEXT"),
      lookup("LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup",
             col("REGION_TRAVEL_AREA_CODE")
      ).as("in_REGION_TRAVEL_AREA_CODE"),
      lookup("LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup", col("BRAND_CODE"))
        .getField("REGION_TRAVEL_AREA_REFNC_CODE")
        .as("REGION_TRAVEL_AREA_REFNC_CODE"),
      lookup("LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup", col("BRAND_CODE"))
        .getField("REGION_TRAVEL_AREA_NAME")
        .as("REGION_TRAVEL_AREA_NAME"),
      lookup("LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup", col("BRAND_CODE"))
        .as("in_BRAND_CODE")
    )

}
