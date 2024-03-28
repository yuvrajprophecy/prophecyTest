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

object trAdjTimeNcheckAcct_V0S919P2_reformat {

  def apply(context: Context, lkPibAcntLoad: DataFrame): DataFrame =
    lkPibAcntLoad.select(
      col("RPS_ACCT_ID_14").as("NATURALKEY"),
      col("PIB_PAGE_NAM"),
      col("APP_NUM"),
      col("SESSN_ID"),
      col("IP_ID_EMPLY"),
      col("GLOBL_SESSN_ID"),
      setnull().as("CRCD_NUM"),
      col("CHANL_ID"),
      col("PIB_ID"),
      when((col("month") >= lit(4.0d)).and(col("month") <= lit(9.0d)),
           col("UKLocalTimeMiliSeconds")
      ).when(
          (col("month") === lit(3.0d))
            .and(
              ds_string_concat(ds_string_concat(col("date2"), lit("")),
                               col("Time")
              ) > ds_string_concat(col("LastSunday"), lit("00.59.59"))
            )
            .or(
              (col("month") === lit(10.0d)).and(
                ds_string_concat(ds_string_concat(col("date2"), lit("")),
                                 col("Time")
                ) <= ds_string_concat(col("LastSunday"), lit("00.59.59"))
              )
            ),
          col("UKLocalTimeMiliSeconds")
        )
        .otherwise(col("TSMiliSeconds"))
        .as("TIMESTAMP"),
      col("TRAN_RNK_ID"),
      col("PIB_PTLT_NAM"),
      col("IP_ID")
    )

}
