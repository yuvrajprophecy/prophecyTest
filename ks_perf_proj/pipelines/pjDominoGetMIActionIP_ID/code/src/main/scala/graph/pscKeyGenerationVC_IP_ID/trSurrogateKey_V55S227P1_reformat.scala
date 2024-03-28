package graph.pscKeyGenerationVC_IP_ID

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.pscKeyGenerationVC_IP_ID.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object trSurrogateKey_V55S227P1_reformat {

  def apply(context: Context, lkNewSKey: DataFrame): DataFrame =
    lkNewSKey.select(col("NATURALKEY"), col("SURROGATEKEY"), col("TRAN_RNK_ID"))

}
