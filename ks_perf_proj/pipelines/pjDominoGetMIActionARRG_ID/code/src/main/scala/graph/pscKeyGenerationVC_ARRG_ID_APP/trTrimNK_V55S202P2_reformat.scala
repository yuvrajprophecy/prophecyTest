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

object trTrimNK_V55S202P2_reformat {

  def apply(context: Context, lkDataIn: DataFrame): DataFrame =
    lkDataIn.select(ds_trim(col("NATURALKEY")).as("NATURALKEY"),
                    col("TRAN_RNK_ID"),
                    col("pXREFTABLE"),
                    col("pNATURALKEY"),
                    col("pSURROGATEKEY")
    )

}
