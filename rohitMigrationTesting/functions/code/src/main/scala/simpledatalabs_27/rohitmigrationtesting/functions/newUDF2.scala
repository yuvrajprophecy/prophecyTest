package simpledatalabs_27.rohitmigrationtesting.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object NewUDF2 extends Serializable {
  var x       = 12
  def newUDF2 = udf((value: Int, value2: Int) => value + value2)
}
