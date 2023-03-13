package rohitjain27simpledatalabs.com_team.pm3rework

import org.apache.spark.sql._
package object functions {
  val trimUDF3 = TrimUDF3.trimUDF3
  val trimUDF1 = TrimUDF1.trimUDF1

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("trimUDF3", trimUDF3)
    spark.udf.register("trimUDF1", trimUDF1)
  }

}
