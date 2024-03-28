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

object luSampleRec {

  def apply(
    context:         Context,
    lkAllData:       DataFrame,
    lkRefSampleRows: DataFrame
  ): DataFrame =
    lkAllData
      .as("lkAllData")
      .join(lkRefSampleRows.as("lkRefSampleRows"),
            col("lkAllData.RowNum") === col("lkRefSampleRows.RowNum"),
            "inner"
      )
      .select(col("lkAllData.ATTRIB_NAME").as("ATTRIB_NAME"),
              col("lkAllData.PIB_ID").as("PIB_ID")
      )

}
