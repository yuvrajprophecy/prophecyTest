package io.prophecy.pipelines.pipeline1.config

import io.prophecy.pipelines.pipeline1.config.ConfigStore._
import io.prophecy.pipelines.pipeline1.config.Context
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  Subgraph_1: Subgraph_1 = Subgraph_1(),
  basesg1_1:  Basesg1_1 = Basesg1_1()
) extends ConfigBase

object Subgraph_1 {

  implicit val confHint: ProductHint[Subgraph_1] =
    ProductHint[Subgraph_1](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Subgraph_1()

object Basesg1_1 {

  implicit val confHint: ProductHint[Basesg1_1] =
    ProductHint[Basesg1_1](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Basesg1_1(config1: String = "defaultvalue")
