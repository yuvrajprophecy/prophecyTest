package io.prophecy.pipelines.perf_pipeline.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.perf_pipeline.graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}

case class Config(Subgraph_1: Subgraph_1_Config = Subgraph_1_Config())
    extends ConfigBase