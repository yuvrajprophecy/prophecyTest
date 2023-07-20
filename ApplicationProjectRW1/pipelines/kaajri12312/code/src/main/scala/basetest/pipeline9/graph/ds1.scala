package basetest.pipeline9.graph

import io.prophecy.libs._
import basetest.pipeline9.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object ds1 {

  def apply(context: Context): DataFrame = {
    import org.apache.avro.Schema
    var reader = context.spark.read.format("avro")
    reader = reader
    reader.load("ddwd")
  }

}
