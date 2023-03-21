package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LKP_SAILING_TYPE_Lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("LKP_SAILING_TYPE_Lookup",
                 in,
                 context.spark,
                 List("in_SAILING_TYPE_CODE"),
                 "SAILING_TYPE_CODE",
                 "SAILING_TYPE_DESC"
    )

}
