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

object soPIBs {

  def apply(context: Context, lkSoPIBs: DataFrame): DataFrame =
    lkSoPIBs.orderBy(col("PIB_ID").asc)

}
