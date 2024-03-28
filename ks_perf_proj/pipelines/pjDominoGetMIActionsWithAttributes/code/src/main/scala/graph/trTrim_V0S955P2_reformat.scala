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

object trTrim_V0S955P2_reformat {

  def apply(context: Context, lkTrimFields: DataFrame): DataFrame =
    lkTrimFields.select(col("PIB_ID"),
                        ds_trim(col("FIELD_NAME")).as("FIELD_NAME")
    )

}
