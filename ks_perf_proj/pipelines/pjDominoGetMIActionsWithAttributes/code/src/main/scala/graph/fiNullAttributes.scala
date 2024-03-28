package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object fiNullAttributes {

  def apply(context: Context, lkCopyDistinctData: DataFrame): DataFrame =
    lkCopyDistinctData.filter(
      lit("ATTRIB_NAME <> '' AND ATTRIB_NAME IS NOT NULL AND PIB_ID > 0")
    )

}
