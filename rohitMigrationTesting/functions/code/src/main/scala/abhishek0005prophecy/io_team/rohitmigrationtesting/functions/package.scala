package abhishek0005prophecy.io_team.rohitmigrationtesting

import org.apache.spark.sql._
package object functions {
  val trimString = TrimString.trimString
  val addNumbers = AddNumbers.addNumbers

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("trimString", trimString)
    spark.udf.register("addNumbers", addNumbers)
  }

}
