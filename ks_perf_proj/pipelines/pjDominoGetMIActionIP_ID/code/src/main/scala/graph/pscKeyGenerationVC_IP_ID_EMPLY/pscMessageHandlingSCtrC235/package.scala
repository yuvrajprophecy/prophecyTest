package graph.pscKeyGenerationVC_IP_ID_EMPLY

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.pscKeyGenerationVC_IP_ID_EMPLY.pscMessageHandlingSCtrC235.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object pscMessageHandlingSCtrC235 {

  def apply(context: Context, LogMessage: DataFrame): Unit = {
    val df_Msg_Tfm_stage_var = Msg_Tfm_stage_var(context, LogMessage)
    val df_Msg_Tfm_V3S0P2_reformat =
      Msg_Tfm_V3S0P2_reformat(context, df_Msg_Tfm_stage_var)
    MessageTgt_FF(context,             df_Msg_Tfm_V3S0P2_reformat)
  }

}
