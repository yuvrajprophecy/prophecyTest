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

object luCheckExists {

  def apply(
    context:          Context,
    lkCheckExists:    DataFrame,
    lkRefCheckExists: DataFrame
  ): DataFrame =
    lkCheckExists
      .as("lkCheckExists")
      .join(lkRefCheckExists.as("lkRefCheckExists"), lit(""), "inner")
      .select(col("lkCheckExists.FIELD_NAME").as("FIELD_NAME"),
              col("lkCheckExists.PIB_ID").as("PIB_ID")
      )

}
