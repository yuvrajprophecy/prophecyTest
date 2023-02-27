package rohitjain27simpledatalabs.com_team.pm3rework

import org.apache.spark.sql._
package object functions {
  val trimUDF  = TrimUDF.trimUDF
  val square   = Square.square
  val square21 = Square21.square21

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("trimUDF",  trimUDF)
    spark.udf.register("square",   square)
    spark.udf.register("square21", square21)
  }

}
