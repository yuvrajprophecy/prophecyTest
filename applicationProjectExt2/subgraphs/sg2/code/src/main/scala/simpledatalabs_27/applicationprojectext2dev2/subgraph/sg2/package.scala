package simpledatalabs_27.applicationprojectext2dev2.subgraph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
import simpledatalabs_27.applicationprojectext2dev2.subgraph.sg2.config._
package object sg2 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_4 = Reformat_4(context, in0)
    val df_Script_2   = Script_2(context,   df_Reformat_4)
    df_Script_2
  }

}
