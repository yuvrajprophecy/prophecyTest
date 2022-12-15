package simpledatalabs_27.applicationprojectext2.functions
import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
object Square extends Serializable { def square = udf { (i: Int) => i * i } }