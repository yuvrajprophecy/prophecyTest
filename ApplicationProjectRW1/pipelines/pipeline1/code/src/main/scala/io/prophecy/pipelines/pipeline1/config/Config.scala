package io.prophecy.pipelines.pipeline1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.pipeline1.graph.basesg1_1.config.{
  Config => basesg1_1_Config
}
import io.prophecy.pipelines.pipeline1.graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}

case class Config(
  Subgraph_1: Subgraph_1_Config = Subgraph_1_Config(),
  basesg1_1:  basesg1_1_Config = basesg1_1_Config()
) extends ConfigBase
