package graph.pscKeyGenerationVC_IP_ID

import io.prophecy.libs._
import graph.pscKeyGenerationVC_IP_ID.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object bopGenTeradataKeys_221 {
  def apply(context: Context, NaturalKeys: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
     // Could not generate the component
     // Name : bopGenTeradataKeys_221
     // Properties :
     /*
     pUKDWUSERNAME -> Some(#pUKDWUSERNAME#)
    Name -> Some(bopGenTeradataKeys_221)
    NextRecordID -> Some(0)
    AllowColumnMapping -> Some(0)
    NextID -> Some(4)
    InputPins -> Some(V55S221P1)
    pUKDWPASSWORD -> Some(#pUKDWPASSWORD#)
    pUKDWSCHEMA -> Some(#pUKDWSCHEMA#)
    pUKDWTEMPFILEPATH -> Some(#pUKDWTEMPFILEPATH#)
    OutputPins -> Some(V55S221P3)
    StageType -> Some(bopGenTeradataKeys)
    pXREFTABLE -> Some(#pXREFTABLE#)
    pNATURALKEY -> Some(#pNATURALKEY#)
     */
    lkNewSKey
  }

}
