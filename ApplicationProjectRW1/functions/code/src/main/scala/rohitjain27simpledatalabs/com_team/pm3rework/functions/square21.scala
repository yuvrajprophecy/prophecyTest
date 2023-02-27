package rohitjain27simpledatalabs.com_team.pm3rework.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Square21 extends Serializable {
  val x1       = 11
  def square21 = udf((value: Int) => value * value)
}
