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

object luMetaData {

  def apply(
    context:         Context,
    lkCheckMetaData: DataFrame,
    lkRefMetaData:   DataFrame
  ): DataFrame =
    lkCheckMetaData
      .as("lkCheckMetaData")
      .join(lkRefMetaData.as("lkRefMetaData"),
            col("lkCheckMetaData.PIB_ID") === col("lkRefMetaData.PIB_ID"),
            "inner"
      )
      .select(
        col("lkCheckMetaData.ATTRIB_NAME").as("ATTRIB_NAME"),
        col("lkCheckMetaData.RowNum").as("RowNum"),
        col("lkCheckMetaData.ATTRIB_VALUE").as("ATTRIB_VALUE"),
        col("lkCheckMetaData.PIB_ID").as("PIB_ID"),
        col("lkCheckMetaData.TRAN_RNK_ID").as("TRAN_RNK_ID"),
        col("lkCheckMetaData.IP_ID").as("IP_ID")
      )

}
