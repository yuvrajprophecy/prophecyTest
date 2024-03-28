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

object trMapCols_V156S2P2_reformat {

  def apply(context: Context, lkMapCols: DataFrame): DataFrame =
    lkMapCols.select(col("svGenericItem").as("GENERIC_ITEM"),
                     col("RowNum"),
                     col("PIB_ID"),
                     col("TRAN_RNK_ID"),
                     col("IP_ID")
    )

}
