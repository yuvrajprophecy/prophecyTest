package simpledatalabs_27.rohitmigrationtesting

import org.apache.spark.sql._
package object functions {
  val addNumbers10 = AddNumbers10.addNumbers10
  val addNumbers11 = AddNumbers11.addNumbers11
  val addNumbers10 = AddNumbers10.addNumbers10
  val addNumbers11 = AddNumbers11.addNumbers11
  val addNumber12  = AddNumber12.addNumber12

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("addNumbers10", addNumbers10)
    spark.udf.register("addNumbers11", addNumbers11)
    spark.udf.register("addNumbers10", addNumbers10)
    spark.udf.register("addNumbers11", addNumbers11)
    spark.udf.register("addNumber12",  addNumber12)
  }

}
