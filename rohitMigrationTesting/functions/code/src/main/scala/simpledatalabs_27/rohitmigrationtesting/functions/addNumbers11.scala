package simpledatalabs_27.rohitmigrationtesting.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object AddNumbers11 extends Serializable {
  var x            = 10
  def addNumbers11 = udf((value: Int, value11: Int) => value + value11)
}
