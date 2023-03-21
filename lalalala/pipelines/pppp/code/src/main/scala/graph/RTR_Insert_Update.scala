package graph

import io.prophecy.libs._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RTR_Insert_Update {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(col("NEW_REC") === lit(1)),
     in.filter(col("CHANGED_FLAG") === lit(1))
    )

}
