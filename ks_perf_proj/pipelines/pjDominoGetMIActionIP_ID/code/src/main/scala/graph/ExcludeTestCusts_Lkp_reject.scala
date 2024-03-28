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

object ExcludeTestCusts_Lkp_reject {

  def apply(
    context:                 Context,
    ExclusionsIn_Lnk_reject: DataFrame,
    lkExcludeTC_reject:      DataFrame
  ): DataFrame =
    ExclusionsIn_Lnk_reject
      .as("ExclusionsIn_Lnk_reject")
      .join(lkExcludeTC_reject.as("ExclusionsIn_Lnk"), lit(""), "inner")
      .select(
        col("lkPostDominoOut.ATTRIB_NAME").as("ATTRIB_NAME"),
        col("lkPostDominoOut.PIB_PAGE_NAM").as("PIB_PAGE_NAM"),
        col("lkPostDominoOut.SESSN_ID").as("SESSN_ID"),
        col("lkPostDominoOut.RECORD").as("RECORD"),
        col("lkPostDominoOut.RPS_ACCT_ID_14").as("RPS_ACCT_ID_14"),
        col("lkPostDominoOut.RowNum").as("RowNum"),
        col("lkPostDominoOut.ATTRIB_VALUE").as("ATTRIB_VALUE"),
        col("lkPostDominoOut.CIN").as("CIN"),
        col("lkPostDominoOut.PIB_ID").as("PIB_ID"),
        col("lkPostDominoOut.TIMESTAMP").as("TIMESTAMP"),
        col("ExclusionsIn_Lnk_reject.TRAN_RNK_ID").as("TRAN_RNK_ID"),
        col("lkPostDominoOut.CIN").as("CDU_CIN"),
        col("lkPostDominoOut.PIB_PTLT_NAM").as("PIB_PTLT_NAM")
      )

}
