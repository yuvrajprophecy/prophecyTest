package rohitjain27simpledatalabs.com_team.pm3rework.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TrimUDF5 extends Serializable {
  val x        = 5
  def trimUDF5 = udf((value5: String) => value5.trim())
}
