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

object moFieldNamesOut {

  def apply(context: Context, lkModFieldsOut: DataFrame): DataFrame =
    lkModFieldsOut.select(
      col("KEY1").as("#pKEYFIELD1# "),
      col("KEY2").as("#pKEYFIELD2# "),
      col("KEY3").as("#pKEYFIELD3# "),
      col("KEY4").as("#pKEYFIELD4# "),
      col("KEY5").as("#pKEYFIELD5# "),
      col("KEY4"),
      col("KEY5"),
      col("KEY1"),
      col("KEY2"),
      col("KEY3")
    )

}
