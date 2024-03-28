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

object trMapCols_V78S0P7_reformat {

  def apply(context: Context, lkMapCols: DataFrame): DataFrame =
    lkMapCols.select(
      col("APP_NUM"),
      col("SESSN_ID"),
      stringtodate(col("pUKDWDATADATE"), lit("%yyyy-%mm-%dd")).as("REC_ACDT"),
      col("PIB_PTLT_NAM").as("PTLT_NAME"),
      col("IP_ID_EMPLY"),
      stringtotimestamp2(
        ds_string_concat(
          datastage_substring(col("TIMESTAMP"), lit(1.0d), lit(11.0d)),
          datastage_substring(
            ds_string_concat(convert3(lit(""),
                                      lit(""),
                                      datastage_substring(col("TIMESTAMP"),
                                                          lit(12.0d),
                                                          lit(15.0d)
                                      )
                             ),
                             lit("00")
            ),
            lit(1.0d),
            lit(26.0d)
          )
        ),
        lit("%yyyy-%mm-%dd %hh:%nn:%ss.6")
      ).as("TRAN_TS"),
      col("PIB_PAGE_NAM").as("PAGE_NAME"),
      col("GLOBL_SESSN_ID"),
      col("CRCD_NUM"),
      col("CHANL_ID"),
      col("ARRG_ID"),
      col("PIB_ID"),
      col("TRAN_RNK_ID"),
      col("IP_ID")
    )

}
