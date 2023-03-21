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

object LKP_V_SALES_PRODUCT_OVERRIDES_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_V_SALES_PRODUCT_OVERRIDES_Lookup",
      in,
      context.spark,
      List("SHIP_CODE1", "SHIP_DEPARTURE_DATE1"),
      "SHIP_CODE",
      "SHIP_DEPARTURE_DATE",
      "OVERRIDE_PRODUCT_CODE"
    )

}
