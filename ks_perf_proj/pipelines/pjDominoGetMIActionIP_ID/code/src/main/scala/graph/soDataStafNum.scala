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

object soDataStafNum {

  def apply(context: Context, lkSoDataStafNum: DataFrame): DataFrame =
    lkSoDataStafNum.orderBy(col("STAF_NUM").asc)

}
