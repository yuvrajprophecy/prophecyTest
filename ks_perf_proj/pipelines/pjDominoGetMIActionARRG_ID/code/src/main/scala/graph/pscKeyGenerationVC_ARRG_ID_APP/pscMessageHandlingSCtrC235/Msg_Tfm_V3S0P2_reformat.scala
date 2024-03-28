package graph.pscKeyGenerationVC_ARRG_ID_APP.pscMessageHandlingSCtrC235

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.pscKeyGenerationVC_ARRG_ID_APP.pscMessageHandlingSCtrC235.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Msg_Tfm_V3S0P2_reformat {

  def apply(context: Context, Message_Move_Lnk: DataFrame): DataFrame =
    Message_Move_Lnk.select(
      col("MSG_TYP").as("DS_MSG_TYP"),
      when(ds_trim(col("DSJobController")) === lit(""), col("DSJobName"))
        .otherwise(col("DSJobController"))
        .as("DS_CNTL_JOB"),
      lit("Msg_Tfm").as("DS_STAG"),
      ds_string_concat(
        ds_string_concat(currenttimestamp(), lit(".")),
        ds_right(ds_string_concat(lit("000000"), lit(context.config.INROWNUM)),
                 lit(6.0d)
        )
      ).as("DS_LOG_MSG_TS"),
      ds_trim(col("DSJobName")).as("DS_JOB_NAM"),
      lit(2.0d).as("CNTL_JOB_RUN_ID"),
      lit(3.0d).as("CNTL_JOB_RRUN_ID"),
      col("MSG_TXT").as("DS_MSG_TXT")
    )

}
