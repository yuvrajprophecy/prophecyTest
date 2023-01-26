package abhishek0005prophecy.io_team.rohitmigrationtesting.pipeline.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {
  var x = 10

  def registerUDFs(spark: SparkSession) =
    abhishek0005prophecy.io_team.rohitmigrationtesting.functions
      .registerFunctions(spark)

}
