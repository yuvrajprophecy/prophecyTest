package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object cpAllPIBs {

  def apply(context: Context, lkPIBon: DataFrame): DataFrame =
    lkPIBon.select(
      col("ATTRIB_NAME"),
      col("PIB_PAGE_NAM"),
      col("APP_NUM"),
      col("STAF_NUM"),
      col("SESSN_ID"),
      col("RECORD"),
      col("RPS_ACCT_ID_14"),
      col("GLOBL_SESSN_ID"),
      col("RowNum"),
      col("ATTRIB_VALUE"),
      col("CIN"),
      col("CHANL_ID"),
      col("SURROGATEKEY").as("PIB_ID"),
      col("TIMESTAMP"),
      col("PIB_PTLT_NAM")
    )

}
