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

object trAdjTimeNcheckAcct_V0S919P2_constraint {

  def apply(context: Context, lkPibAcntLoad: DataFrame): DataFrame =
    lkPibAcntLoad.filter(col("svHSBCAccount"))

}
