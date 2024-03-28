package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object dsLoadData {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("quote",  "\"")
      .option("sep",    ",")
      .schema(StructType(Array()))
      .load(
        "${pUKDWTEMPFILEPATH}/${pSUBJECT}_${$DB2_DATABASE}_${$DB2_SCHEMA}_${pTABLE_NAME}_${pPIB_ID}.ds"
      )

}
