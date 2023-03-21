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

object LKP_V_SALES_PRODUCT_OVERRIDES {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_SALES_PRODUCT_OVERRIDES_Lookup", col("SHIP_DEPARTURE_DATE"))
        .as("SHIP_DEPARTURE_DATE1"),
      lookup("LKP_V_SALES_PRODUCT_OVERRIDES_Lookup", col("SHIP_CODE"))
        .as("SHIP_CODE1"),
      lookup("LKP_V_SALES_PRODUCT_OVERRIDES_Lookup", col("SHIP_DEPARTURE_DATE"))
        .getField("OVERRIDE_PRODUCT_CODE")
        .as("OVERRIDE_PRODUCT_CODE")
    )

}
