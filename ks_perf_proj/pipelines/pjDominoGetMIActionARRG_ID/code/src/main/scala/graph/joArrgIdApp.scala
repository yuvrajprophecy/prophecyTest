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

object joArrgIdApp {

  def apply(
    context:        Context,
    lkGetArrgIdApp: DataFrame,
    lkArrgIdApp:    DataFrame
  ): DataFrame =
    lkGetArrgIdApp
      .as("lkGetArrgIdApp")
      .join(lkArrgIdApp.as("lkArrgIdApp"),
            col("lkGetArrgIdApp.APP_NUM") === col("lkArrgIdApp.APP_NUM"),
            "leftouter"
      )
      .select(
        col("lkGetArrgIdApp.SESSN_ID").as("SESSN_ID"),
        col("lkArrgIdApp.ARRG_ID_APP").as("ARRG_ID_APP"),
        col("lkGetArrgIdApp.REC_ACDT").as("REC_ACDT"),
        col("lkGetArrgIdApp.PTLT_NAME").as("PTLT_NAME"),
        col("lkGetArrgIdApp.IP_ID_EMPLY").as("IP_ID_EMPLY"),
        col("lkGetArrgIdApp.TRAN_TS").as("TRAN_TS"),
        col("lkGetArrgIdApp.PAGE_NAME").as("PAGE_NAME"),
        col("lkGetArrgIdApp.GLOBL_SESSN_ID").as("GLOBL_SESSN_ID"),
        col("lkGetArrgIdApp.CRCD_NUM").as("CRCD_NUM"),
        col("lkGetArrgIdApp.CHANL_ID").as("CHANL_ID"),
        col("lkGetArrgIdApp.ARRG_ID").as("ARRG_ID"),
        col("lkGetArrgIdApp.PIB_ID").as("PIB_ID"),
        col("lkGetArrgIdApp.TRAN_RNK_ID").as("TRAN_RNK_ID"),
        col("lkGetArrgIdApp.IP_ID").as("IP_ID")
      )

}
