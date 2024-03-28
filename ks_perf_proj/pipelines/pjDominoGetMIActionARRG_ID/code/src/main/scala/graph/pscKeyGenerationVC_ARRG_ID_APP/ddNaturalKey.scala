package graph.pscKeyGenerationVC_ARRG_ID_APP

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.pscKeyGenerationVC_ARRG_ID_APP.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object ddNaturalKey {

  def apply(context: Context, lkGetGenKeys: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    lkGetGenKeys
      .withColumn(
        "row_number",
        row_number().over(Window.partitionBy("NATURALKEY").orderBy(lit(1)))
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
