package basetest.pipeline91.graph

import io.prophecy.libs._
import basetest.pipeline91.config.Context
import basetest.pipeline91.udfs.UDFs._
import basetest.pipeline91.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Lookup_2 {

  def apply(context: Context, in0: DataFrame): Unit =
    createLookup("lookup_1",
                 in0,
                 context.spark,
                 List("customer_id"),
                 "first_name"
    )

}
