package simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_2 = Reformat_2(spark, in0)
    val df_Reformat_3 = Reformat_3(spark, df_Reformat_2)
    df_Reformat_3
  }

}
