package graph.pscKeyGenerationVC_PIB_ID

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.pscKeyGenerationVC_PIB_ID.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object luAttachNewKeys {

  def apply(
    context:         Context,
    lkAttachNewKeys: DataFrame,
    lkAllRecsOut:    DataFrame
  ): DataFrame =
    lkAttachNewKeys
      .as("lkAttachNewKeys")
      .join(
        lkAllRecsOut.as("lkAttachNewKeys"),
        col("lkAllRecsOut.NATURALKEY") === col("lkAttachNewKeys.NATURALKEY"),
        "inner"
      )
      .select(col("lkAttachNewKeys.SURROGATEKEY").as("SURROGATEKEY"),
              col("lkAllRecsOut.NATURALKEY").as("NATURALKEY")
      )

}
