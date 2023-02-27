package rohitjain27simpledatalabs.com_team.pm3rework

import org.apache.spark.sql._
package object functions {
  val trimUDF   = TrimUDF.trimUDF
  val square    = Square.square
  val square212 = Square212.square212

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("trimUDF",   trimUDF)
    spark.udf.register("square",    square)
    spark.udf.register("square212", square212)
  }

}
