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

object ciParseGenericItem {
  def apply(context: Context, lkGetMetaData1: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
     // Could not generate the component
     // Name : ciParseGenericItem
     // Properties :
     /*
     schemafile -> Some(#pUKDWTEMPFILEPATH#/#pSUBJECT#_#pPIB_ID#_#pUKDWSCHEMA#_#pTABLE_NAME#.sch)
    Name -> Some(ciParseGenericItem)
    field -> Some(GENERIC_ITEM)
    NextRecordID -> Some(0)
    AllowColumnMapping -> Some(0)
    NextID -> Some(5)
    InputPins -> Some(V0S921P1)
    keepField -> Some(\(20))
    schema -> Some(\(2)\(2)0)
    selection -> Some(file)
    OutputPins -> Some(V0S921P2)
    StageType -> Some(PxColumnImport)
    saveRejects\failRejects -> Some(\(20))
     */
    lkOutputData
  }

}
