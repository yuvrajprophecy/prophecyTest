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

object ddRemovDupPibs {

  def apply(context: Context, lkRemoveDups: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    lkRemoveDups
      .withColumn(
        "row_number",
        row_number().over(Window.partitionBy("PIB_ID").orderBy(lit(1)))
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
