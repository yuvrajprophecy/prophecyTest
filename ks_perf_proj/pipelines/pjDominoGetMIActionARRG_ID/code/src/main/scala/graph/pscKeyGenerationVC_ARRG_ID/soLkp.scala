package graph.pscKeyGenerationVC_ARRG_ID

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.pscKeyGenerationVC_ARRG_ID.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object soLkp {

  def apply(context: Context, lkLookupSurrogateKey: DataFrame): DataFrame =
    lkLookupSurrogateKey.orderBy(col("NATURALKEY").asc)

}
