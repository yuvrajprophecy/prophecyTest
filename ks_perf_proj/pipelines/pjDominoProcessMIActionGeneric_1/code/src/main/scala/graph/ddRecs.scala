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

object ddRecs {

  def apply(context: Context, lkDeDupe: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    lkDeDupe
      .withColumn("row_number",
                  row_number().over(
                    Window
                      .partitionBy("RowNum", "PIB_ID", "IP_ID", "TRAN_RNK_ID")
                      .orderBy(lit(1))
                  )
      )
      .withColumn(
        "count",
        count("*").over(
          Window.partitionBy("RowNum", "PIB_ID", "IP_ID", "TRAN_RNK_ID")
        )
      )
      .filter(col("row_number") === col("count"))
      .drop("row_number")
      .drop("count")
  }

}
