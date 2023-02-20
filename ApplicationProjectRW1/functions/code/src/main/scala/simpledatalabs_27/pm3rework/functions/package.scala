package simpledatalabs_27.pm3rework

import org.apache.spark.sql._
package object functions {
  val square = Square.square
  val trim   = Trim.trim

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("square", square)
    spark.udf.register("trim",   trim)
  }

}
