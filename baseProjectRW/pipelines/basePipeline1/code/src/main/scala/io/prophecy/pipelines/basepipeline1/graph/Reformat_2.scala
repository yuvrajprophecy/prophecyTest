package io.prophecy.pipelines.basepipeline1.graph

import io.prophecy.libs._
import io.prophecy.pipelines.basepipeline1.udfs.UDFs._
import io.prophecy.pipelines.basepipeline1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("email"),
      col("country_code"),
      col("account_open_date"),
      col("account_flags"),
      to_timestamp(lit("06-24-2019 12:01:19.000"), "MM-dd-yyyy HH:mm:ss.SSSS")
        .as("ts1")
    )

}
