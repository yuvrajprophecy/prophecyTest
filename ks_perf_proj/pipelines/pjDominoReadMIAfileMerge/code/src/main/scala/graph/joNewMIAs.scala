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

object joNewMIAs {

  def apply(
    context:       Context,
    lkJoinNewMIAs: DataFrame,
    lkNewMIAs:     DataFrame
  ): DataFrame =
    lkJoinNewMIAs
      .as("lkJoinNewMIAs")
      .join(lkNewMIAs.as("lkNewMIAs"),
            col("lkJoinNewMIAs.RowNum") === col("lkNewMIAs.RowNum"),
            "rightouter"
      )
      .select(
        col("lkJoinNewMIAs.ATTRIB_NAME").as("ATTRIB_NAME"),
        col("lkNewMIAs.PIB_PAGE_NAM").as("PIB_PAGE_NAM"),
        col("lkNewMIAs.APP_NUM").as("APP_NUM"),
        col("lkNewMIAs.STAF_NUM").as("STAF_NUM"),
        col("lkNewMIAs.SESSN_ID").as("SESSN_ID"),
        col("lkNewMIAs.RECORD").as("RECORD"),
        col("lkNewMIAs.RPS_ACCT_ID_14").as("RPS_ACCT_ID_14"),
        col("lkNewMIAs.PIB_MIA_COD").as("PIB_MIA_COD"),
        col("lkNewMIAs.GLOBL_SESSN_ID").as("GLOBL_SESSN_ID"),
        col("lkNewMIAs.RowNum").as("RowNum"),
        col("lkJoinNewMIAs.ATTRIB_VALUE").as("ATTRIB_VALUE"),
        col("lkNewMIAs.CIN").as("CIN"),
        col("lkNewMIAs.CHANL_ID").as("CHANL_ID"),
        col("lkNewMIAs.TIMESTAMP").as("TIMESTAMP"),
        col("lkNewMIAs.PIB_PTLT_NAM").as("PIB_PTLT_NAM")
      )

}
