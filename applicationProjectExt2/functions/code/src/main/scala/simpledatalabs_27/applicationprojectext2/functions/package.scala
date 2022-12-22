package simpledatalabs_27.applicationprojectext2

import org.apache.spark.sql._
package object functions {
  // main.  
  val square  = Square.square
  val square2 = Square2.square2

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("square",  square)
    spark.udf.register("square2", square2)
  }

}
