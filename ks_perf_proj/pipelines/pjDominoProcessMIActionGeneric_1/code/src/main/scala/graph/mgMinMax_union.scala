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

object mgMinMax_union {

  def apply(
    context:  Context,
    lkRefMin: DataFrame,
    lkRefMax: DataFrame
  ): DataFrame =
    List(lkRefMin, lkRefMax).flatMap(Option(_)).reduce(_.unionAll(_))

}
