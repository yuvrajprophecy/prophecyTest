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

object LKP_V_ACTIVE_STATUS_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_V_ACTIVE_STATUS_Lookup",
      in,
      context.spark,
      List("in_ACTIVE_STATUS_CODE"),
      "ACTIVE_STATUS_CODE",
      "ACTIVE_STATUS_DESC",
      "CREATION_DTM",
      "CREATION_USER_ID",
      "LAST_UPDATE_DTM",
      "LAST_UPDATE_USER_ID"
    )

}
