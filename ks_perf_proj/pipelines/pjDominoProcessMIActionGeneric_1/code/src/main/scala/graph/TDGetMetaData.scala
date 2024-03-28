package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TDGetMetaData {

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
      s"SELECT PIB_ID FROM DWH_PIB_ACTN_DESC WHERE PIB_ID = ${Config.pPIB_ID}"
    )
    var df = reader.load()
    df
  }

}
