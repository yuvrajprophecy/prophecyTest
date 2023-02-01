package simpledatalabs_27.rohitmigrationtesting.pipeline.pipeline2.graph

import io.prophecy.libs._
import simpledatalabs_27.rohitmigrationtesting.pipeline.pipeline2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object DS1 {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header",                                   true)
      .option("sep",                                      ",")
      .schema(StructType(Array(StructField("customer_id", StringType, true))))
      .load("dede")

}
