package rohitjain27simpledatalabs.com_team.pm3rework

import org.apache.spark.sql._
package object functions {
  val trimUDF = TrimUDF.trimUDF

  def registerFunctions(spark: SparkSession) =
    spark.udf.register("trimUDF", trimUDF)

}
