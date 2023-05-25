package basetest.pipeline9.graph.sdfsd.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import basetest.pipeline9.graph.sdfsd.Subgraph_1.config.{
  Config => Subgraph_1_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(Subgraph_1: Subgraph_1_Config = Subgraph_1_Config())
    extends ConfigBase

case class Context(spark: SparkSession, config: Config)
