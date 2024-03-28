package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object dsIP_IDs {

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
            StructField("PIB_PAGE_NAM",   StringType,  true),
            StructField("APP_NUM",        StringType,  true),
            StructField("SESSN_ID",       StringType,  true),
            StructField("RPS_ACCT_ID_14", StringType,  true),
            StructField("IP_ID_EMPLY",    IntegerType, true),
            StructField("GLOBL_SESSN_ID", StringType,  true),
            StructField("CHANL_ID",       StringType,  true),
            StructField("PIB_ID",         IntegerType, true),
            StructField("pUKDWDATADATE",  StringType,  true),
            StructField("TIMESTAMP",      StringType,  true),
            StructField("TRAN_RNK_ID",    IntegerType, true),
            StructField("PIB_PTLT_NAM",   StringType,  true),
            StructField("IP_ID",          IntegerType, true)
          )
        )
      )
      .load(
        s"${Config.pUKDWTEMPFILEPATH}/${Config.pCLEANSEDDATASET}_DOMINO_IP.ds"
      )
  }

}
