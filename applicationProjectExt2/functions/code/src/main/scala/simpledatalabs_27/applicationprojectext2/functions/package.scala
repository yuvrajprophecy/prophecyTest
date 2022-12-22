package simpledatalabs_27.applicationprojectext2

import org.apache.spark.sql._
package object functions {
  val square  = Square.square
  val square2 = Square2.square2
  val square4 = Square4.square4

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("square",  square)
    spark.udf.register("square2", square2)
    spark.udf.register("square4", square4)
  }

}
