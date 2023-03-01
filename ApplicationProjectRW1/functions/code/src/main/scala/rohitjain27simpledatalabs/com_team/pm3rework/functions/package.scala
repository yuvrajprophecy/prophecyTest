package rohitjain27simpledatalabs.com_team.pm3rework

import org.apache.spark.sql._
package object functions {
  val trimUDF  = TrimUDF.trimUDF
  val trimUDF2 = TrimUDF2.trimUDF2

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("trimUDF",  trimUDF)
    spark.udf.register("trimUDF2", trimUDF2)
  }

}
