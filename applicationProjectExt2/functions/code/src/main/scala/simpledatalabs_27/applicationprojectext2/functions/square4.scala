package simpledatalabs_27.applicationprojectext2.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Square4 extends Serializable {
  def square4 = udf((i41: Int) => i41 * i41)
}
