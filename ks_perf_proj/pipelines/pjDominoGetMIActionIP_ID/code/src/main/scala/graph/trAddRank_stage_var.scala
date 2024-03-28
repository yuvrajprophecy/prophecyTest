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

object trAddRank_stage_var {

  def apply(context: Context, lkAddRank: DataFrame): DataFrame =
    lkAddRank.withColumn("svRank",
                         when(col("clusterKeyChange"), lit(1.0d))
                           .otherwise(col("svRank") + lit(1.0d))
    )

}
