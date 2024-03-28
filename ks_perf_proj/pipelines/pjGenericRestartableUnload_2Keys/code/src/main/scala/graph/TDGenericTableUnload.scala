package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TDGenericTableUnload {

  def apply(context: Context): DataFrame = {
    val Config = context.config
    var reader = context.spark.read.format("jdbc")
    reader = reader
      .option("url",               s"${Config.pUKDWSCHEMA}")
      .option("user",              s"${Config.pUKDWUSERNAME}")
      .option("password",          s"${Config.pUKDWPASSWORD}")
      .option("pushDownPredicate", true)
      .option("driver",            "com.tera.teraDriver")
    reader = reader.option(
      "query",
      s"SELECT ${Config.pKEYFIELD1} AS KEY1, ${Config.pKEYFIELD2} AS KEY2 FROM ${Config.pTABLE_NAME} WHERE ('${Config.pRERUN}' = 'Y' or ('${Config.pFORCE_LOOKUP}' = 'Yes' and '${Config.pLOOKUP_DATASET_ALREADY_EXISTS}' <> 'Yes' ) ) ${Config.pOPTIONAL_FILTER}"
    )
    var df = reader.load()
    df
  }

}
