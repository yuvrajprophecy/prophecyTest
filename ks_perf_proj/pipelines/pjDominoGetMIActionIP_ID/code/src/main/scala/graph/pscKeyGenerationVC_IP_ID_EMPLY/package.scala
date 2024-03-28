package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.pscKeyGenerationVC_IP_ID_EMPLY.config._
import graph.pscKeyGenerationVC_IP_ID_EMPLY.pscMessageHandlingSCtrC235
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object pscKeyGenerationVC_IP_ID_EMPLY {

  def apply(context: Context, lkGenStafNumIP: DataFrame): DataFrame = {
    val df_dsNaturalKeyToSurrogateKey = dsNaturalKeyToSurrogateKey(context)
    val df_soLkp                      = soLkp(context,              df_dsNaturalKeyToSurrogateKey)
    val df_trTrimNK_stage_var         = trTrimNK_stage_var(context, lkGenStafNumIP)
    val df_trTrimNK_V55S202P2_reformat =
      trTrimNK_V55S202P2_reformat(context, df_trTrimNK_stage_var)
    val df_soRecs                 = soRecs(context,                 df_trTrimNK_V55S202P2_reformat)
    val df_joXref                 = joXref(context,                 df_soRecs, df_soLkp)
    val df_trCheckFound_stage_var = trCheckFound_stage_var(context, df_joXref)
    val df_trCheckFound_V55S171P7_constraint =
      trCheckFound_V55S171P7_constraint(context, df_trCheckFound_stage_var)
    val df_trCheckFound_V55S171P1_constraint =
      trCheckFound_V55S171P1_constraint(context, df_trCheckFound_stage_var)
    val df_trCheckFound_V55S171P1_reformat = trCheckFound_V55S171P1_reformat(
      context,
      df_trCheckFound_V55S171P1_constraint
    )
    val df_trCheckFound_V55S171P7_reformat = trCheckFound_V55S171P7_reformat(
      context,
      df_trCheckFound_V55S171P7_constraint
    )
    val df_ddNaturalKey =
      ddNaturalKey(context, df_trCheckFound_V55S171P7_reformat)
    val df_bopGenTeradataKeys_221 =
      bopGenTeradataKeys_221(context, df_ddNaturalKey)
    val df_trCheckFound_V55S171P4_constraint =
      trCheckFound_V55S171P4_constraint(context, df_trCheckFound_stage_var)
    val df_trCheckFound_V55S171P4_reformat = trCheckFound_V55S171P4_reformat(
      context,
      df_trCheckFound_V55S171P4_constraint
    )
    val df_moDropSurrogeteKey =
      moDropSurrogeteKey(context, df_trCheckFound_V55S171P4_reformat)
    val df_trSurrogateKey_stage_var =
      trSurrogateKey_stage_var(context, df_bopGenTeradataKeys_221)
    val df_trSurrogateKey_V55S227P1_reformat =
      trSurrogateKey_V55S227P1_reformat(context, df_trSurrogateKey_stage_var)
    val df_luAttachNewKeys = luAttachNewKeys(
      context,
      df_trSurrogateKey_V55S227P1_reformat,
      df_moDropSurrogeteKey
    )
    val df_fuAll =
      fuAll(context, df_luAttachNewKeys, df_trCheckFound_V55S171P1_reformat)
    val df_trSurrogateKey_V55S227P3_reformat =
      trSurrogateKey_V55S227P3_reformat(context, df_trSurrogateKey_stage_var)
    pscMessageHandlingSCtrC235.apply(
      pscMessageHandlingSCtrC235.config
        .Context(context.spark, context.config.pscMessageHandlingSCtrC235),
      df_trSurrogateKey_V55S227P3_reformat
    )
    df_fuAll
  }

}
