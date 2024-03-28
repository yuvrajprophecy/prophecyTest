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

object ExcludeTestCusts_Lkp {

  def apply(
    context:          Context,
    ExclusionsIn_Lnk: DataFrame,
    lkExcludeTC:      DataFrame
  ): DataFrame =
    ExclusionsIn_Lnk
      .as("ExclusionsIn_Lnk")
      .join(lkExcludeTC.as("ExclusionsIn_Lnk"), lit(""), "inner")
      .select(col("lkExcludeTC.SESSN_ID").as("SESSN_ID"),
              col("ExclusionsIn_Lnk.pUKDWDATADATE").as("pUKDWDATADATE")
      )

}
