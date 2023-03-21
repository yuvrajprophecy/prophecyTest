package simpledatalabs_27.baseprojectrw.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object CreateFullName3 extends Serializable {
  val x = 4

  def createFullName3 =
    udf((value: String, value2: String) => value + value2)

}
