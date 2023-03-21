package simpledatalabs_27.baseprojectrw.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object CreateFullName2 extends Serializable {
  val x = 3

  def createFullName2 =
    udf((value1: String, value2: String) => value1 + value2)

}
