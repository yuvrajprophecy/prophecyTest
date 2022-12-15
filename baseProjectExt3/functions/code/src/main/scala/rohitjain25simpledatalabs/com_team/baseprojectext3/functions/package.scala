package rohitjain25simpledatalabs.com_team.baseprojectext3
import org.apache.spark.sql._
package object functions {
  val sharedSquare = SharedSquare.sharedSquare
  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("sharedSquare", sharedSquare)
  }
}