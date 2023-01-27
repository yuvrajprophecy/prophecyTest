package simpledatalabs_27.rohitmigrationtesting

import org.apache.spark.sql._
package object functions {
  val trimString   = TrimString.trimString
  val addNumbers   = AddNumbers.addNumbers
  val addNumbers1  = AddNumbers1.addNumbers1
  val addNumbers2  = AddNumbers2.addNumbers2
  val addNumbers3  = AddNumbers3.addNumbers3
  val addNumbers4  = AddNumbers4.addNumbers4
  val addNumbers5  = AddNumbers5.addNumbers5
  val addNumbers6  = AddNumbers6.addNumbers6
  val addNumbers7  = AddNumbers7.addNumbers7
  val addNumbers8  = AddNumbers8.addNumbers8
  val addNumbers9  = AddNumbers9.addNumbers9
  val addNumbers10 = AddNumbers10.addNumbers10
  val addNumbers11 = AddNumbers11.addNumbers11

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("trimString",   trimString)
    spark.udf.register("addNumbers",   addNumbers)
    spark.udf.register("addNumbers1",  addNumbers1)
    spark.udf.register("addNumbers2",  addNumbers2)
    spark.udf.register("addNumbers3",  addNumbers3)
    spark.udf.register("addNumbers4",  addNumbers4)
    spark.udf.register("addNumbers5",  addNumbers5)
    spark.udf.register("addNumbers6",  addNumbers6)
    spark.udf.register("addNumbers7",  addNumbers7)
    spark.udf.register("addNumbers8",  addNumbers8)
    spark.udf.register("addNumbers9",  addNumbers9)
    spark.udf.register("addNumbers10", addNumbers10)
    spark.udf.register("addNumbers11", addNumbers11)
  }

}
