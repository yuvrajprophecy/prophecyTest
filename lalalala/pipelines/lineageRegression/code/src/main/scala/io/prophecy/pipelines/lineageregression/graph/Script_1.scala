package io.prophecy.pipelines.lineageregression.graph

import io.prophecy.libs._
import io.prophecy.pipelines.lineageregression.config.Context
import io.prophecy.pipelines.lineageregression.udfs.UDFs._
import io.prophecy.pipelines.lineageregression.udfs._
import io.prophecy.pipelines.lineageregression.udfs.PipelineInitCode._
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
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    
    val out0 = in0.withColumn("prediction1", lit(Vectors.dense(1.0, 0.0, 3.0)))
    out0
  }

}
