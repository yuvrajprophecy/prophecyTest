package graph.pscKeyGenerationVC_IP_ID

import io.prophecy.libs._
import graph.pscKeyGenerationVC_IP_ID.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object dsNaturalKeyToSurrogateKey {

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
            StructField("NATURALKEY",    StringType,  true),
            StructField("SURROGATEKEY",  IntegerType, true),
            StructField("TRAN_RNK_ID",   StringType,  true),
            StructField("pXREFTABLE",    StringType,  true),
            StructField("pNATURALKEY",   StringType,  true),
            StructField("pSURROGATEKEY", StringType,  true)
          )
        )
      )
      .load(
        s"${Config.pSHRDREFDATAPATH}/${Config.pUKDWSCHEMA}_${Config.pXREFTABLE}_${Config.pNATURALKEY}_${Config.pDS_TIMESTAMP}.ds"
      )
  }

}
