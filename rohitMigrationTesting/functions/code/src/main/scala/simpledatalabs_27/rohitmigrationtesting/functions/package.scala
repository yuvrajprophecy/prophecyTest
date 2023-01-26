package simpledatalabs_27.rohitmigrationtesting

import org.apache.spark.sql._
package object functions {
  val addNumbers = AddNumbers.addNumbers
  val newUDF1    = NewUDF1.newUDF1
  val newUDF2    = NewUDF2.newUDF2

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("addNumbers", addNumbers)
    spark.udf.register("newUDF1",    newUDF1)
    spark.udf.register("newUDF2",    newUDF2)
  }

}
