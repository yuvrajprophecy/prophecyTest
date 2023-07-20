package basetest.pipeline92.graph.Subgraph_1.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  boolean_config: Boolean = true,
  string_config:  String = "default_str",
  double_config:  Double = 1.0d,
  array_config:   List[String] = List("1"),
  record_config:  Record_config = Record_config()
) extends ConfigBase

object Record_config {

  implicit val confHint: ProductHint[Record_config] =
    ProductHint[Record_config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Record_config(
  record_string_config: String = "record_string_config",
  record_float_config:  Float = 2.0f
)

case class Context(spark: SparkSession, config: Config)
