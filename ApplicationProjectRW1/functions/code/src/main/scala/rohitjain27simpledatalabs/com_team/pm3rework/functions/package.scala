package rohitjain27simpledatalabs.com_team.pm3rework

import org.apache.spark.sql._
package object functions {
  val trimUDF3 = TrimUDF3.trimUDF3
  val trimUDF  = TrimUDF.trimUDF
  val trimUDF1 = TrimUDF1.trimUDF1
  val trimUDF2 = TrimUDF2.trimUDF2

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("trimUDF3", trimUDF3)
    spark.udf.register("trimUDF",  trimUDF)
    spark.udf.register("trimUDF1", trimUDF1)
    spark.udf.register("trimUDF2", trimUDF2)
  }

}
