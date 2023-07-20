package basetest.pipeline92.graph

import io.prophecy.libs._
import basetest.pipeline92.graph.Subgraph_1.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {

  def apply(context: Context, in: DataFrame, in00: DataFrame): DataFrame = {
    Lookup_1(context, in00)
    val df_Reformat_1 = Reformat_1(context, in)
    val df_Script_1   = Script_1(context,   df_Reformat_1)
    df_Script_1
  }

}
