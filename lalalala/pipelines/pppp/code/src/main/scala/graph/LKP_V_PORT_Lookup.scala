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

object LKP_V_PORT_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_V_PORT_Lookup",
      in,
      context.spark,
      List("in_PORT_CODE"),
      "PORT_CODE",
      "PORT_NAME",
      "COUNTRY_CODE",
      "CURRENCY_CODE",
      "CURRENCY_2_CODE",
      "AIRPORT_CODE",
      "HANDICAP_ACCESS_DESC",
      "AUTOBOOK_DAYS_START_QTY",
      "AUTOBOOK_DAYS_STOP_QTY",
      "DAYS_ADD_PSEUDO_NAMES_QTY",
      "DAYS_CHANGE_CXL_MAN_MODE_QTY",
      "AUTO_BOOK_AVAILABLE_FLAG",
      "SHORE_EXCURSION_PORT_CODE",
      "APIS_PORT_CODE",
      "LAST_CHANGED_DTM",
      "POLCDT",
      "POLCTM",
      "LAST_CHANGED_USER_ID",
      "JOB_NAME",
      "JOB_NBR"
    )

}
