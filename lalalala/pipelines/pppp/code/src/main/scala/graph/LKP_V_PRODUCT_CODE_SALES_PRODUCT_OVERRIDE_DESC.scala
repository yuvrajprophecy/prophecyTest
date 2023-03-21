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

object LKP_V_PRODUCT_CODE_SALES_PRODUCT_OVERRIDE_DESC {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_PRODUCT_CODE_SALES_PRODUCT_OVERRIDE_DESC_Lookup",
             col("OVERRIDE_PRODUCT_CODE")
      ).getField("PRODUCT_DESC").as("PRODUCT_DESC"),
      lookup("LKP_V_PRODUCT_CODE_SALES_PRODUCT_OVERRIDE_DESC_Lookup",
             col("OVERRIDE_PRODUCT_CODE")
      ).as("PRODUCT_CODE1")
    )

}
