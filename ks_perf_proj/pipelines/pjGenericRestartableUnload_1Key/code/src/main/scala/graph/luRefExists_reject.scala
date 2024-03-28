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

object luRefExists_reject {

  def apply(
    context:              Context,
    lkRefExists_reject:   DataFrame,
    lkCheckExists_reject: DataFrame
  ): DataFrame =
    lkRefExists_reject
      .as("lkRefExists_reject")
      .join(lkCheckExists_reject.as("lkRefExists"),
            col("lkCheckExists.KEY1") === col("lkRefExists.KEY1"),
            "inner"
      )
      .select(col("lkRefExists_reject.KEY1").as("KEY1"))

}
