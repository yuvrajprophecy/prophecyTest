package io.prophecy.pipelines.pipeline1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.pipeline1.graph.sg1.config.{Config => sg1_Config}

case class Config(
  Subgraph_1: SUBGRAPH_1 = SUBGRAPH_1(),
  basesg1_1:  Basesg1_1 = Basesg1_1(),
  sg1:        sg1_Config = sg1_Config()
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
