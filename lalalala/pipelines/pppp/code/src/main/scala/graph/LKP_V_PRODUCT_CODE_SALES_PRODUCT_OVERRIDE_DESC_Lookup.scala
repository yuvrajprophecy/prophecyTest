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

object LKP_V_PRODUCT_CODE_SALES_PRODUCT_OVERRIDE_DESC_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("LKP_V_PRODUCT_CODE_SALES_PRODUCT_OVERRIDE_DESC_Lookup",
                 in,
                 context.spark,
                 List("PRODUCT_CODE1"),
                 "PRODUCT_CODE",
                 "PRODUCT_DESC"
    )

}
