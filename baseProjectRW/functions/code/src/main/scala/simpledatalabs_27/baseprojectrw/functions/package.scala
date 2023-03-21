package simpledatalabs_27.baseprojectrw

import org.apache.spark.sql._
package object functions {
  val createFullName  = CreateFullName.createFullName
  val createFullName2 = CreateFullName2.createFullName2
  val createFullName1 = CreateFullName1.createFullName1

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("createFullName",  createFullName)
    spark.udf.register("createFullName2", createFullName2)
    spark.udf.register("createFullName1", createFullName1)
  }

}
