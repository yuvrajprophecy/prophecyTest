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

object joXref {

  def apply(context: Context, lkData: DataFrame, lkLkp: DataFrame): DataFrame =
    lkData
      .as("lkData")
      .join(lkLkp.as("lkLkp"),
            col("lkData.NATURALKEY") === col("lkLkp.NATURALKEY"),
            "leftouter"
      )
      .select(
        col("lkData.NATURALKEY").as("NATURALKEY"),
        col("lkLkp.SURROGATEKEY").as("SURROGATEKEY"),
        col("lkData.TRAN_RNK_ID").as("TRAN_RNK_ID"),
        col("lkData.pXREFTABLE").as("pXREFTABLE"),
        col("lkData.pNATURALKEY").as("pNATURALKEY"),
        col("lkData.pSURROGATEKEY").as("pSURROGATEKEY")
      )

}
