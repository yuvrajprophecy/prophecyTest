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

object Join_1 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(
        in1.as("in1"),
        col("in0.ts1") < col("in1.ts1") - lit(Duration.ofNanos(1800000000000L)),
        "inner"
      )

}
