package simpledatalabs_27.pm3rework

import org.apache.spark.sql._
package object functions {
  val square  = Square.square
  val trimUDF = TrimUDF.trimUDF

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("square",  square)
    spark.udf.register("trimUDF", trimUDF)
  }

}
