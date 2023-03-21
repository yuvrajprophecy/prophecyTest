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

object EXP_Get_NEXTVAL {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("IN_STRING").as("STRING_in"),
      concat(
        col("vNEXTVAL_prefix"),
        lit(0),
        (date_format(to_timestamp(col("SESSSTARTTIME"), "yyY"), "yyY")
          .or(date_format(to_timestamp(col("SESSSTARTTIME"), "EEE"), "EEE"))
          .cast(DoubleType) + lit(100000)) * lit(1000000000),
        col("vNEXTVAL_prefix")
      ).as("vNEXTVAL_prefix"),
      (col("vNEXTVAL_prefix") + col("NEXTVAL_in")).as("NEXTVAL"),
      col("NEXTVAL_in")
    )

}
