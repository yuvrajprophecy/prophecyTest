package rohitjain27simpledatalabs.com_team.pm3rework.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TrimUDF3 extends Serializable {
  val pipeline3 = "ldme"
  def trimUDF3  = udf((value3: String) => value3.trim())
}
