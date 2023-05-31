package basetest.pipeline9111.graph

import io.prophecy.libs._
import basetest.pipeline9111.config.Context
import basetest.pipeline9111.udfs.UDFs._
import basetest.pipeline9111.udfs._
import basetest.pipeline9111.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_1 {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val out0 = in0
    out0
  }

}
