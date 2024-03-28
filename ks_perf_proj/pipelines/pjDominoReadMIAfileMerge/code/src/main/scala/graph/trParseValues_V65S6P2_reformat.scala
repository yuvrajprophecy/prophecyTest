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

object trParseValues_V65S6P2_reformat {

  def apply(context: Context, lkLandDataToSF: DataFrame): DataFrame =
    lkLandDataToSF.select(
      field3(col("RECORD"), lit("~"), lit(1.0d)).as("RowNum"),
      col("svATTRIBNAME").as("ATTRIB_NAME"),
      field3(col("RECORD"), lit("~"), lit(3.0d)).as("ATTRIB_VALUE")
    )

}
