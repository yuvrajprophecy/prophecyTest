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

object cpMapCols {

  def apply(context: Context, lkARRG_ID: DataFrame): DataFrame =
    lkARRG_ID.select(
      col("PIB_PAGE_NAM"),
      col("APP_NUM"),
      col("SESSN_ID"),
      col("IP_ID_EMPLY"),
      col("GLOBL_SESSN_ID"),
      col("CRCD_NUM"),
      col("CHANL_ID"),
      col("SURROGATEKEY").as("ARRG_ID"),
      col("PIB_ID"),
      col("pUKDWDATADATE"),
      col("TIMESTAMP"),
      col("TRAN_RNK_ID"),
      col("PIB_PTLT_NAM"),
      col("IP_ID")
    )

}
