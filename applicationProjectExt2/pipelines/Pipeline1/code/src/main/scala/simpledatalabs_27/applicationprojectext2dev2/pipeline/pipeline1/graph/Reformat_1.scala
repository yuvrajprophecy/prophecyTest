package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.graph

import io.prophecy.libs._
import simpledatalabs_27.applicationprojectext2dev2.functions._
import _simpledatalabs_25.baseproject2.functions._
import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(square(col("customer_id")).as("id"),
              udfShared2(col("customer_id")).as("id2")
    )

}
