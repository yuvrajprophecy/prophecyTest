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

object lkParseRowNum_V65S1P4_reformat {

  def apply(context: Context, lkReadOrigData: DataFrame): DataFrame =
    lkReadOrigData.select(
      field3(col("RECORD"), lit("|"), lit(1.0d)).as("RowNum"),
      col("RECORD")
    )

}
