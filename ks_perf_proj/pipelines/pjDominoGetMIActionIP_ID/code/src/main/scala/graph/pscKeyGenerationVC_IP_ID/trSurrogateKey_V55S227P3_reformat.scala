package graph.pscKeyGenerationVC_IP_ID

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.pscKeyGenerationVC_IP_ID.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object trSurrogateKey_V55S227P3_reformat {

  def apply(context: Context, lkNewSKey: DataFrame): DataFrame =
    lkNewSKey.select(
      lit("1").as("MSG_TYP"),
      ds_string_concat(
        ds_string_concat(
          ds_string_concat(
            ds_string_concat(
              ds_string_concat(
                ds_string_concat(
                  ds_string_concat(
                    ds_string_concat(ds_string_concat(lit("Xreftable = "),
                                                      ds_trim(col("pXREFTABLE"))
                                     ),
                                     lit(",")
                    ),
                    ds_trim(col("pNATURALKEY"))
                  ),
                  lit("= ")
                ),
                ds_trim(col("NATURALKEY"))
              ),
              lit(", ")
            ),
            ds_trim(col("pSURROGATEKEY"))
          ),
          lit("= ")
        ),
        ds_trim(col("SURROGATEKEY"))
      ).as("MSG_TXT")
    )

}
