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

object trHandleNulls_V0S833P2_reformat {

  def apply(context: Context, lkMergedRecords: DataFrame): DataFrame =
    lkMergedRecords.select(
      col("PIB_PAGE_NAM"),
      nulltoempty(col("ID")).as("ID"),
      col("APP_NUM"),
      col("STAF_NUM"),
      nulltoempty(col("SESSN_ID")).as("SESSN_ID"),
      col("RECORD"),
      col("RPS_ACCT_ID_14"),
      col("PIB_MIA_COD"),
      col("GLOBL_SESSN_ID"),
      col("RowNum"),
      nulltoempty(col("CIN")).as("CIN"),
      col("CHANL_ID"),
      col("TIMESTAMP"),
      col("PIB_PTLT_NAM")
    )

}
