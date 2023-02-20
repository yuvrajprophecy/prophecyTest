package simpledatalabs_27.pm3rework.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Trim extends Serializable {
  val y    = 10
  def trim = udf((value: String) => value.trim())
}
