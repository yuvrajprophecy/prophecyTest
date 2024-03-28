package graph.pscKeyGenerationVC_IP_ID.pscMessageHandlingSCtrC235

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.pscKeyGenerationVC_IP_ID.pscMessageHandlingSCtrC235.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Msg_Tfm_stage_var {

  def apply(context: Context, Message_Move_Lnk: DataFrame): DataFrame =
    Message_Move_Lnk

}
