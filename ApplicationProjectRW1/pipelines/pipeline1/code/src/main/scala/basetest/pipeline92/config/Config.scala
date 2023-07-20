package basetest.pipeline92.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import basetest.pipeline92.graph.Subgraph_1.config.{Config => Subgraph_1_Config}

case class Config(
  boolean_config: Boolean = true,
  string_config:  String = "default_str",
  double_config:  Double = 1.0d,
  array_config:   List[String] = List("1"),
  record_config:  Record_config = Record_config(),
  Subgraph_1:     Subgraph_1_Config = Subgraph_1_Config()
) extends ConfigBase

object Record_config {

  implicit val confHint: ProductHint[Record_config] =
    ProductHint[Record_config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Record_config(
  record_string_config: String = "record_string_config",
  record_float_config:  Float = 2.0f
)
