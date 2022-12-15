package rohitjain25simpledatalabs.com_team.baseprojectext3.pipeline.basep.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) =
    rohitjain25simpledatalabs.com_team.baseprojectext3.functions
      .registerFunctions(spark)

}
