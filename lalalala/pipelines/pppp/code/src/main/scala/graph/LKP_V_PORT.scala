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

object LKP_V_PORT {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_V_PORT_Lookup", col("VOYAGE_ORIGINATING_PORT_CODE"))
        .getField("PORT_NAME")
        .as("PORT_NAME"),
      lookup("LKP_V_PORT_Lookup", col("VOYAGE_ORIGINATING_PORT_CODE"))
        .as("in_PORT_CODE")
    )

}
