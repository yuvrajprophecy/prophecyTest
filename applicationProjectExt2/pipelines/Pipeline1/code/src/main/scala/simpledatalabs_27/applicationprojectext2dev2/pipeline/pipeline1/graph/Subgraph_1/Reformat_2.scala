package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.graph.Subgraph_1

import io.prophecy.libs._
import simpledatalabs_27.applicationprojectext2dev2.functions._
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.graph.Subgraph_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(square(col("id")).as("id"))

}
