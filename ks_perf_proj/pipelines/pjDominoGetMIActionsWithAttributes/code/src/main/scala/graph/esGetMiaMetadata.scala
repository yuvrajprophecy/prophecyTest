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

object esGetMiaMetadata {
  def apply(context: Context): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
     // Could not generate the component
     // Name : esGetMiaMetadata
     // Properties :
     /*
     Name -> Some(esGetMiaMetadata)
    NextRecordID -> Some(0)
    AllowColumnMapping -> Some(0)
    NextID -> Some(2)
    OutputPins -> Some(V77S16P1)
    StageType -> Some(PxExternalSource)
     */
    lkCheckExists
  }

}
