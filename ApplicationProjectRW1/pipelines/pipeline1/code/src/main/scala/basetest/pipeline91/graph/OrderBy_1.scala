package basetest.pipeline91.graph

import io.prophecy.libs._
import basetest.pipeline91.udfs.UDFs._
import basetest.pipeline91.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OrderBy_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("customer_id").asc)

}
