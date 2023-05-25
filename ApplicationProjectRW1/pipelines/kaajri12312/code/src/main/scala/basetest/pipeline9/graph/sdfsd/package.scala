package basetest.pipeline9.graph

import io.prophecy.libs._
import basetest.pipeline9.graph.sdfsd.config._
import basetest.pipeline9.graph.sdfsd.Subgraph_1
import basetest.pipeline9.graph.sdfsd.Subgraph_1.config.{
  Context => Subgraph_1_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object sdfsd {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1_Context(context.spark, context.config.Subgraph_1),
      in0
    )
  }

}
