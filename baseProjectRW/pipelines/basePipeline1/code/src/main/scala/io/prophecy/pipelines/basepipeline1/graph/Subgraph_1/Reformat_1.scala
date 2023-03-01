package io.prophecy.pipelines.basepipeline1.graph.Subgraph_1

import io.prophecy.libs._
import io.prophecy.pipelines.basepipeline1.udfs.UDFs._
import io.prophecy.pipelines.basepipeline1.graph.Subgraph_1.config.Context
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
