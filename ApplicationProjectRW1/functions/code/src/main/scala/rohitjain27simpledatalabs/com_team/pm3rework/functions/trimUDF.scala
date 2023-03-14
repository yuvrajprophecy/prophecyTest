package rohitjain27simpledatalabs.com_team.pm3rework.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TrimUDF extends Serializable {
  val pipeline3 = "ldme"
  def trimUDF   = udf((value3: String) => value3.trim())
}
