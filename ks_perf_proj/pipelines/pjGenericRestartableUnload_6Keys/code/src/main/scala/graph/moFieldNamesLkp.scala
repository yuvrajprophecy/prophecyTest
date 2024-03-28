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

object moFieldNamesLkp {

  def apply(context: Context, lkBuildLookupFileset: DataFrame): DataFrame =
    lkBuildLookupFileset.select(col("KEY1"),
                                col("KEY2"),
                                col("KEY3"),
                                col("KEY4"),
                                col("KEY5"),
                                col("KEY6")
    )

}
