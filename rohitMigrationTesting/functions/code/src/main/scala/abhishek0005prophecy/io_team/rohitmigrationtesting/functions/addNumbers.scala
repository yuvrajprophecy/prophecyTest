package abhishek0005prophecy.io_team.rohitmigrationtesting.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object AddNumbers extends Serializable {
  var x          = 10
  def addNumbers = udf((value: Int, value2: Int) => value + value2)
}
