package simpledatalabs_27.applicationprojectext2
import org.apache.spark.sql._
package object functions {
  val square = Square.square
  def registerFunctions(spark: SparkSession) = { oueee
    spark.udf.register("square", square)
  }
}
