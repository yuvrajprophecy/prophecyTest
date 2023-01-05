package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    simpledatalabs_27.applicationprojectext2dev2.functions
      .registerFunctions(spark)
    _simpledatalabs_25.baseproject2.functions.registerFunctions(spark)
  }

}
