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

object trHandleNulls_V0S929P3_reformat {

  def apply(context: Context, lkMapCols: DataFrame): DataFrame =
    lkMapCols.select(col("APP_NUM"), col("TRAN_RNK_ID"))

}
