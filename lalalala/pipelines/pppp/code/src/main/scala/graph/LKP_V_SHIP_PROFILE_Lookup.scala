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

object LKP_V_SHIP_PROFILE_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_V_SHIP_PROFILE_Lookup",
      in,
      context.spark,
      List("in_SHIP_CODE", "in_SHIP_PROFILE_ID"),
      "SHIP_CODE",
      "SHIP_PROFILE_ID",
      "SHIP_PROFILE_DESC",
      "ACTIVE_STATUS_CODE",
      "AUTO_UPGR_ELIG_FROM_CATG_CODE",
      "AUTO_UPGR_ELIG_TO_CATG_CODE",
      "CREATION_DTM",
      "HVCRDT",
      "HVCRTM",
      "CREATION_USER_ID",
      "LAST_CHANGED_DTM",
      "HVLCDT",
      "HVLCTM",
      "LAST_CHANGED_USER_ID",
      "JOB_NBR"
    )

}
