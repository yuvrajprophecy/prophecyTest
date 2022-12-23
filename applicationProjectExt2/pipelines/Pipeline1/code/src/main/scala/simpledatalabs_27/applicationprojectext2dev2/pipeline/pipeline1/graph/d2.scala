package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.graph

import io.prophecy.libs._
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object d2 {

  def apply(spark: SparkSession, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("error")
      .save("d2")

}
