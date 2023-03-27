package simpledatalabs_27.baseprojectrw

import org.apache.spark.sql._
package object functions {
  val createFullName = CreateFullName.createFullName

  def registerFunctions(spark: SparkSession) =
    spark.udf.register("createFullName", createFullName)

}
