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

object luMetaData_reject {

  def apply(
    context:                Context,
    lkCheckMetaData_reject: DataFrame,
    lkRefMetaData_reject:   DataFrame
  ): DataFrame =
    lkCheckMetaData_reject
      .as("lkCheckMetaData_reject")
      .join(lkRefMetaData_reject.as("lkRefMetaData"),
            col("lkCheckMetaData.PIB_ID") === col("lkRefMetaData.PIB_ID"),
            "inner"
      )
      .select(
        col("lkCheckMetaData_reject.ATTRIB_NAME").as("ATTRIB_NAME"),
        col("lkCheckMetaData_reject.RowNum").as("RowNum"),
        col("lkCheckMetaData_reject.ATTRIB_VALUE").as("ATTRIB_VALUE"),
        col("lkCheckMetaData_reject.PIB_ID").as("PIB_ID"),
        col("lkCheckMetaData_reject.TRAN_RNK_ID").as("TRAN_RNK_ID"),
        col("lkCheckMetaData_reject.IP_ID").as("IP_ID")
      )

}
