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

object trAttachDelimiter_V0S994P3_reformat {

  def apply(context: Context, lkAttachDelimiter: DataFrame): DataFrame =
    lkAttachDelimiter.select(
      ds_string_concat(lit("~"), col("ATTRIB_NAME")).as("ATTRIB_NAME"),
      ds_string_concat(lit("~"), col("MIN_VALUE")).as("MIN_VALUE"),
      ds_string_concat(lit("~"), col("MAX_VALUE")).as("MAX_VALUE")
    )

}
