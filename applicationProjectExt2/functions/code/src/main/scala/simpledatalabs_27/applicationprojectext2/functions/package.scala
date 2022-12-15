package simpledatalabs_27.applicationprojectext2
package object functions {
  val square = Square.square
  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("square", square)
  }
}