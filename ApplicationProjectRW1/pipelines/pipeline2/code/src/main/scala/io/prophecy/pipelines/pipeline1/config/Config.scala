package io.prophecy.pipelines.pipeline1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.pipeline1.graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}
import io.prophecy.pipelines.pipeline1.graph.basesg1_1.config.{
  Config => basesg1_1_Config
}

case class Config(
  Subgraph_1: SUBGRAPH_1 = SUBGRAPH_1(),
  basesg1_1:  Basesg1_1 = Basesg1_1()
) extends ConfigBase

object SUBGRAPH_1 {

  implicit val confHint: ProductHint[SUBGRAPH_1] =
    ProductHint[SUBGRAPH_1](ConfigFieldMapping(CamelCase, CamelCase))

}

case class SUBGRAPH_1()

object Basesg1_1 {

  implicit val confHint: ProductHint[Basesg1_1] =
    ProductHint[Basesg1_1](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Basesg1_1(config1: String = "defaultvalue")
