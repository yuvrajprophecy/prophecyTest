package graph.pscKeyGenerationVC_ARRG_ID_APP

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.pscKeyGenerationVC_ARRG_ID_APP.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object soRecs {

  def apply(context: Context, lkSoNK: DataFrame): DataFrame =
    lkSoNK.orderBy(col("NATURALKEY").asc)

}
