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

object joStafNum {

  def apply(
    context:       Context,
    lkDataStafNum: DataFrame,
    lkLkpStafNum:  DataFrame
  ): DataFrame =
    lkDataStafNum
      .as("lkDataStafNum")
      .join(lkLkpStafNum.as("lkLkpStafNum"),
            col("lkDataStafNum.STAF_NUM") === col("lkLkpStafNum.STAF_NUM"),
            "leftouter"
      )
      .select(
        col("lkDataStafNum.PIB_PAGE_NAM").as("PIB_PAGE_NAM"),
        col("lkDataStafNum.APP_NUM").as("APP_NUM"),
        col("lkDataStafNum.SESSN_ID").as("SESSN_ID"),
        col("lkDataStafNum.RPS_ACCT_ID_14").as("RPS_ACCT_ID_14"),
        col("lkLkpStafNum.IP_ID_EMPLY").as("IP_ID_EMPLY"),
        col("lkDataStafNum.GLOBL_SESSN_ID").as("GLOBL_SESSN_ID"),
        col("lkDataStafNum.CHANL_ID").as("CHANL_ID"),
        col("lkDataStafNum.PIB_ID").as("PIB_ID"),
        col("lkDataStafNum.TIMESTAMP").as("TIMESTAMP"),
        col("lkDataStafNum.TRAN_RNK_ID").as("TRAN_RNK_ID"),
        col("lkDataStafNum.PIB_PTLT_NAM").as("PIB_PTLT_NAM"),
        col("lkDataStafNum.IP_ID").as("IP_ID")
      )

}
