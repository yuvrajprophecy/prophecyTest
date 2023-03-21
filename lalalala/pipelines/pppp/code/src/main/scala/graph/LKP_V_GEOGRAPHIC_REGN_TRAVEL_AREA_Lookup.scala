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

object LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup",
      in,
      context.spark,
      List("in_GEOGRAPHIC_REGION_CODE",
           "in_REGION_TRAVEL_AREA_CODE",
           "in_BRAND_CODE"
      ),
      "GEOGRAPHIC_REGION_CODE",
      "REGION_TRAVEL_AREA_CODE",
      "BRAND_CODE",
      "REGION_TRAVEL_AREA_REFNC_CODE",
      "REGION_TRAVEL_AREA_NAME",
      "REGION_TRAVEL_AREA_TEXT",
      "REGN_TRAVL_AREA_SAIL_PERD_TEXT",
      "REGION_TRAVL_AREA_CLIMATE_TEXT",
      "REGION_TRAVEL_AREA_VERSION_NBR",
      "CREATION_DTM",
      "LWCRDT",
      "LWCRTM",
      "CREATION_USER_ID",
      "LAST_CHANGED_DTM",
      "LWLCDT",
      "LWLCTM",
      "LAST_CHANGED_USER_ID"
    )

}
