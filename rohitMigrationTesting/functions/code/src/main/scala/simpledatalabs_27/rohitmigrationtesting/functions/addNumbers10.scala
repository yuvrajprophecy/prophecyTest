package simpledatalabs_27.rohitmigrationtesting.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object AddNumbers10 extends Serializable {
  var x            = 10
  def addNumbers10 = udf((value: Int, value10: Int) => value + value10)
}
