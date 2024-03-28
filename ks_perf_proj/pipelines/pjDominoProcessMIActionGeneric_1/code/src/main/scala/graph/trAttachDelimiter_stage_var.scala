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

object trAttachDelimiter_stage_var {

  def apply(context: Context, lkAttachDelimiter: DataFrame): DataFrame =
    lkAttachDelimiter

}
