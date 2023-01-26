package simpledatalabs_27.rohitmigrationtesting.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object NewUDF1 extends Serializable {
  var x       = 11
  def newUDF1 = udf((value: Int, value2: Int) => value + value2)
}
