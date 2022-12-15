package rohitjain25simpledatalabs.com_team.baseprojectext3
package object functions {
  val sharedSquare = SharedSquare.sharedSquare
  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("sharedSquare", sharedSquare)
  }
}