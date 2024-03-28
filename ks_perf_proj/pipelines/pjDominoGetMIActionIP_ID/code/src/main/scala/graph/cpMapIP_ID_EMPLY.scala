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

object cpMapIP_ID_EMPLY {

  def apply(context: Context, lkIpIdEmply: DataFrame): DataFrame =
    lkIpIdEmply.select(col("NATURALKEY").as("STAF_NUM"),
                       col("SURROGATEKEY").as("IP_ID_EMPLY")
    )

}
