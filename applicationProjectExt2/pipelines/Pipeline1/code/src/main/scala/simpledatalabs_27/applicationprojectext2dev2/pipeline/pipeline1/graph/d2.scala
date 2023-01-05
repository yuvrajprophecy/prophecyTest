package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.graph

import io.prophecy.libs._
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object d2 {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("overwrite")
      .save("d2")

}
