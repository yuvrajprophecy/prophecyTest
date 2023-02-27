package simpledatalabs_27.baseprojectrw

import org.apache.spark.sql._
package object functions {
  val square2  = Square2.square2
  val square22 = Square22.square22

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("square2",  square2)
    spark.udf.register("square22", square22)
  }

}
