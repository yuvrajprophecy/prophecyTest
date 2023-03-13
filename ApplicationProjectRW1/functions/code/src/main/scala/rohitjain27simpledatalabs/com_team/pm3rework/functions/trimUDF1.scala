package rohitjain27simpledatalabs.com_team.pm3rework.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TrimUDF1 extends Serializable {
  val x        = 3
  def trimUDF1 = udf((value3: String) => value3.trim())
}
