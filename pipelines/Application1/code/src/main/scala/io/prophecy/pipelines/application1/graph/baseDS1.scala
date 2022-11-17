package io.prophecy.pipelines.application1.graph

import io.prophecy.libs._
import io.prophecy.pipelines.application1.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object baseDS1 {

  def apply(spark: SparkSession): DataFrame =
    spark.read
      .format("csv")
      .option("header",                            true)
      .option("sep",                               ",")
      .schema(StructType(Array(StructField("eded", StringType, true))))
      .load("dede")

}
