package basetest.pipeline9111.graph

import io.prophecy.libs._
import basetest.pipeline9111.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object baseDS1 {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("error")
      .save("dbfs:/Prophecy/123@mm.com/CustomersDatasetInput.csv")

}
