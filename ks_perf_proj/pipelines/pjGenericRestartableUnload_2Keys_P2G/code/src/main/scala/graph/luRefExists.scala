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

object luRefExists {

  def apply(
    context:       Context,
    lkRefExists:   DataFrame,
    lkCheckExists: DataFrame
  ): DataFrame =
    lkRefExists
      .as("lkRefExists")
      .join(
        lkCheckExists.as("lkRefExists"),
        (col("lkCheckExists.KEY1") === col("lkRefExists.KEY1"))
          .and(col("lkCheckExists.KEY2") === col("lkRefExists.KEY2")),
        "inner"
      )
      .select(col("lkCheckExists.KEY2").as("KEY2"),
              col("lkCheckExists.KEY1").as("KEY1")
      )

}
