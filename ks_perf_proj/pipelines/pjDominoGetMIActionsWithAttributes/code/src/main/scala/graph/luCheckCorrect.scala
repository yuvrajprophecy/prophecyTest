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

object luCheckCorrect {

  def apply(
    context:           Context,
    lkRefCheckCorrect: DataFrame,
    lkCheckCorrect:    DataFrame
  ): DataFrame =
    lkRefCheckCorrect
      .as("lkRefCheckCorrect")
      .join(lkCheckCorrect.as("lkRefCheckCorrect"), lit(""), "inner")
      .select(col("lkCheckCorrect.FIELD_NAME").as("FIELD_NAME"),
              col("lkCheckCorrect.PIB_ID").as("PIB_ID")
      )

}
