package simpledatalabs_27.applicationprojectext2

import org.apache.spark.sql._
package object functions {
  val square = Square.square

  def registerFunctions(spark: SparkSession) =
    spark.udf.register("square", square)

}
