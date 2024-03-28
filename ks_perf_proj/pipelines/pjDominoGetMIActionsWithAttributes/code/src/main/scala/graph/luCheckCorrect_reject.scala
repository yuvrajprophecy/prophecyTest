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

object luCheckCorrect_reject {

  def apply(
    context:                  Context,
    lkRefCheckCorrect_reject: DataFrame,
    lkCheckCorrect_reject:    DataFrame
  ): DataFrame =
    lkRefCheckCorrect_reject
      .as("lkRefCheckCorrect_reject")
      .join(lkCheckCorrect_reject.as("lkRefCheckCorrect"), lit(""), "inner")
      .select(col("lkRefCheckCorrect_reject.FIELD_NAME").as("FIELD_NAME"),
              col("lkRefCheckCorrect_reject.PIB_ID").as("PIB_ID")
      )

}
