package simpledatalabs_27.applicationprojectext2dev2.subgraph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
import simpledatalabs_27.applicationprojectext2dev2.subgraph.sg1.config._
package object sg1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_2 = Reformat_2(context, in0)
    val df_Reformat_3 = Reformat_3(context, df_Reformat_2)
    df_Reformat_3
  }

}
