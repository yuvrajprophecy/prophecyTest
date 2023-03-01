package io.prophecy.pipelines.basepipeline1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.basepipeline1.graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}

case class Config(
  Subgraph_1:  Subgraph_1_Config = Subgraph_1_Config(),
  limit_count: Int = 10,
  string_conf: String = "eddea"
) extends ConfigBase
