package io.prophecy.pipelines.lineageregression.graph

import io.prophecy.libs._
import io.prophecy.pipelines.lineageregression.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object normalization_out {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("overwrite")
      .save("dbfs:/FileStore/tables/rohit/Toshiba/normalization_out.csv")

}
