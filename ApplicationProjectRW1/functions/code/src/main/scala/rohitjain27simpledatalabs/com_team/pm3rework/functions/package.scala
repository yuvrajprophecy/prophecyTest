package rohitjain27simpledatalabs.com_team.pm3rework

import org.apache.spark.sql._
package object functions {
  val trimUDF1 = TrimUDF1.trimUDF1

  def registerFunctions(spark: SparkSession) =
    spark.udf.register("trimUDF1", trimUDF1)

}
