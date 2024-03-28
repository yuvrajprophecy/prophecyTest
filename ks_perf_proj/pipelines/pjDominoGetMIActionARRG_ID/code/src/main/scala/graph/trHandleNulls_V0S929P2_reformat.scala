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

object trHandleNulls_V0S929P2_reformat {

  def apply(context: Context, lkMapCols: DataFrame): DataFrame =
    lkMapCols.select(
      col("PIB_PAGE_NAM"),
      col("APP_NUM"),
      col("SESSN_ID"),
      nulltoempty(col("RPS_ACCT_ID_14")).as("RPS_ACCT_ID_14"),
      col("IP_ID_EMPLY"),
      col("GLOBL_SESSN_ID"),
      col("CHANL_ID"),
      col("PIB_ID"),
      col("pUKDWDATADATE"),
      nulltoempty(col("TIMESTAMP")).as("TIMESTAMP"),
      col("TRAN_RNK_ID"),
      col("PIB_PTLT_NAM"),
      col("IP_ID")
    )

}
