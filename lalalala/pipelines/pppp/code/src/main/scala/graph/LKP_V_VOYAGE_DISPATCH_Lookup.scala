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

object LKP_V_VOYAGE_DISPATCH_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_V_VOYAGE_DISPATCH_Lookup",
      in,
      context.spark,
      List("in_VOYAGE_DISPATCH_CODE"),
      "VOYAGE_DISPATCH_CODE",
      "VOYAGE_DISPATCH_CODE_PK",
      "VOYAGE_DISPATCH_DESC",
      "LAST_CHANGED_JOB_NAME",
      "LAST_CHANGED_JOB_NBR",
      "LAST_CHANGED_TIME"
    )

}
