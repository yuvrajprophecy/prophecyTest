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

object trAddCduCin_V0S927P1_reformat {

  def apply(context: Context, lkPostDominoOut: DataFrame): DataFrame =
    lkPostDominoOut.select(
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
      col("PIB_ID"),
      col("pUKDWDATADATE"),
      col("TIMESTAMP"),
      col("TRAN_RNK_ID"),
      asinteger(ds_trim(col("CIN"))).as("CDU_CIN"),
      col("PIB_PTLT_NAM")
    )

}
