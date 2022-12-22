package simpledatalabs_27.applicationprojectext2dev.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Square2 extends Serializable {
  def square2 = udf((i2: Int) => i2 * i2)
}
