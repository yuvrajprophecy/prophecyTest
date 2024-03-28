package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object awkP2G00003OrigWithRowNum_FF {
  def apply(context: Context): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
     // Could not generate the component
     // Name : awkP2G00003OrigWithRowNum_FF
     // Properties :
     /*
     Name -> Some(awkP2G00003OrigWithRowNum_FF)
    NextRecordID -> Some(0)
    AllowColumnMapping -> Some(0)
    NextID -> Some(2)
    OutputPins -> Some(V67S0P1)
    StageType -> Some(PxExternalSource)
     */
    lkReadOrigData
  }

}
