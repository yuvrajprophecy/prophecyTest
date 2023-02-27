package rohitjain27simpledatalabs.com_team.pm3rework.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Square1 extends Serializable {
  val pipeline2 = "ldme"
  def square1   = udf((value: Int) => value * value)
}
