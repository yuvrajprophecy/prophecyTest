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

object LKP_V_SHIP_PROFILE {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_SHIP_PROFILE_Lookup", col("SHIP_PROFILE_ID"))
        .as("in_SHIP_PROFILE_ID"),
      lookup("LKP_V_SHIP_PROFILE_Lookup", col("SHIP_CODE"))
        .getField("AUTO_UPGR_ELIG_TO_CATG_CODE")
        .as("AUTO_UPGR_ELIG_TO_CATG_CODE"),
      lookup("LKP_V_SHIP_PROFILE_Lookup", col("SHIP_CODE"))
        .getField("AUTO_UPGR_ELIG_FROM_CATG_CODE")
        .as("AUTO_UPGR_ELIG_FROM_CATG_CODE"),
      lookup("LKP_V_SHIP_PROFILE_Lookup", col("SHIP_CODE"))
        .getField("SHIP_PROFILE_DESC")
        .as("SHIP_PROFILE_DESC"),
      lookup("LKP_V_SHIP_PROFILE_Lookup", col("SHIP_CODE")).as("in_SHIP_CODE")
    )

}
