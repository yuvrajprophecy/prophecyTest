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

object trCheckFound_stage_var {

  def apply(context: Context, lkCheckLkp: DataFrame): DataFrame =
    lkCheckLkp
      .withColumn("svNaturalKey", ds_trim(col("NATURALKEY")))
      .withColumn("svTestFound",  nulltozero(col("SURROGATEKEY")) =!= lit(0.0d))

}
