package simpledatalabs_27.baseprojectrw.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object CreateFullName extends Serializable {
  def createFullName = udf((a: String, b: String) => a + ' ' + b)
}
