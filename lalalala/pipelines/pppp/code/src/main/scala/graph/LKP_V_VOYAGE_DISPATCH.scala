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

object LKP_V_VOYAGE_DISPATCH {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_VOYAGE_DISPATCH_Lookup", col("VOYAGE_DISPATCH_CODE"))
        .getField("VOYAGE_DISPATCH_DESC")
        .as("VOYAGE_DISPATCH_DESC"),
      lookup("LKP_V_VOYAGE_DISPATCH_Lookup", col("VOYAGE_DISPATCH_CODE"))
        .as("in_VOYAGE_DISPATCH_CODE")
    )

}
