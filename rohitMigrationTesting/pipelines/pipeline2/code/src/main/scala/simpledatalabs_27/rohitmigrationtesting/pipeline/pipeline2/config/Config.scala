package simpledatalabs_27.rohitmigrationtesting.pipeline.pipeline2.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import simpledatalabs_27.rohitmigrationtesting.pipeline.pipeline2.graph

case class Config(
  Subgraph_1: graph.Subgraph_1.config.Config = graph.Subgraph_1.config.Config()
) extends ConfigBase
