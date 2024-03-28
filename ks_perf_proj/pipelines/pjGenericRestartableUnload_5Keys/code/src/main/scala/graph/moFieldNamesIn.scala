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

object moFieldNamesIn {

  def apply(context: Context, lkModFields: DataFrame): DataFrame = {
    val Config = context.config
    lkModFields.select(
      lit(Config.pKEYFIELD1).as("KEY1"),
      lit(Config.pKEYFIELD2).as("KEY2"),
      lit(Config.pKEYFIELD3).as("KEY3"),
      lit(Config.pKEYFIELD4).as("KEY4"),
      lit(Config.pKEYFIELD5).as("KEY5")
    )
  }

}
