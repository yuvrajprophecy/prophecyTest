package rohitjain27simpledatalabs.com_team.pm3rework

import org.apache.spark.sql._
package object functions {
  val trimUDF4 = TrimUDF4.trimUDF4
  val trimUDF  = TrimUDF.trimUDF

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("trimUDF4", trimUDF4)
    spark.udf.register("trimUDF",  trimUDF)
  }

}
