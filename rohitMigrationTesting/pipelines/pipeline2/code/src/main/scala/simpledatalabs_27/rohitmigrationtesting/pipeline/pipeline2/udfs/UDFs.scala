package simpledatalabs_27.rohitmigrationtesting.pipeline.pipeline2.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {
  var x = 10

  def registerUDFs(spark: SparkSession) =
    simpledatalabs_27.rohitmigrationtesting.functions.registerFunctions(spark)

}
