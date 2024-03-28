package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object dsForDetailProcessing {

  def apply(context: Context): DataFrame = {
    val Config = context.config
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("quote",  "\"")
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("ATTRIB_NAME",   StringType,  true),
            StructField("RowNum",        IntegerType, true),
            StructField("ATTRIB_VALUE",  StringType,  true),
            StructField("PIB_ID",        IntegerType, true),
            StructField("pUKDWDATADATE", StringType,  true),
            StructField("TRAN_RNK_ID",   IntegerType, true),
            StructField("IP_ID",         IntegerType, true)
          )
        )
      )
      .load(
        s"${Config.pUKDWTEMPFILEPATH}/${Config.pCLEANSEDDATASET}_DOMINO_IDS_ON.ds"
      )
  }

}
