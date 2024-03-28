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

object joIPnRank {

  def apply(
    context:            Context,
    lkGetIPnRank:       DataFrame,
    lkUpdateDataStream: DataFrame
  ): DataFrame =
    lkGetIPnRank
      .as("lkGetIPnRank")
      .join(lkUpdateDataStream.as("lkUpdateDataStream"),
            col("lkGetIPnRank.RowNum") === col("lkUpdateDataStream.RowNum"),
            "leftouter"
      )
      .select(
        col("lkGetIPnRank.ATTRIB_NAME").as("ATTRIB_NAME"),
        col("lkGetIPnRank.RowNum").as("RowNum"),
        col("lkGetIPnRank.ATTRIB_VALUE").as("ATTRIB_VALUE"),
        col("lkGetIPnRank.PIB_ID").as("PIB_ID"),
        col("lkUpdateDataStream.TRAN_RNK_ID").as("TRAN_RNK_ID"),
        col("lkUpdateDataStream.IP_ID").as("IP_ID")
      )

}
