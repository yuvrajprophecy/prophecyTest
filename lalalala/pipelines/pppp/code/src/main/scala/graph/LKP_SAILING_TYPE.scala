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

object LKP_SAILING_TYPE {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lookup("LKP_SAILING_TYPE_Lookup", col("SAILING_TYPE_CODE"))
        .as("in_SAILING_TYPE_CODE"),
      lookup("LKP_SAILING_TYPE_Lookup", col("SAILING_TYPE_CODE"))
        .getField("SAILING_TYPE_DESC")
        .as("SAILING_TYPE_DESC")
    )

}
