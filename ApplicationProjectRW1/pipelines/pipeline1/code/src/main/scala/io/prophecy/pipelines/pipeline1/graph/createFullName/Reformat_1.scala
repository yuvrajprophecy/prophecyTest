package io.prophecy.pipelines.pipeline1.graph.createFullName

import io.prophecy.libs._
import io.prophecy.pipelines.pipeline1.udfs.UDFs._
import io.prophecy.pipelines.pipeline1.graph.createFullName.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      createFullName(col("first_name"), col("last_name")).as("full_name")
    )

}
