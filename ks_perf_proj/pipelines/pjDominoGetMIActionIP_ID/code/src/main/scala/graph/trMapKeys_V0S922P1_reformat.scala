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

object trMapKeys_V0S922P1_reformat {

  def apply(context: Context, lkFunnelPost: DataFrame): DataFrame =
    lkFunnelPost.select(
      asinteger(ds_trim(col("CIN"))).as("NATURALKEY"),
      col("PIB_PAGE_NAM"),
      col("APP_NUM"),
      col("STAF_NUM"),
      col("SESSN_ID"),
      col("RECORD"),
      col("RPS_ACCT_ID_14"),
      col("GLOBL_SESSN_ID"),
      col("RowNum"),
      col("CHANL_ID"),
      col("PIB_ID"),
      col("TIMESTAMP"),
      col("PIB_PTLT_NAM")
    )

}
