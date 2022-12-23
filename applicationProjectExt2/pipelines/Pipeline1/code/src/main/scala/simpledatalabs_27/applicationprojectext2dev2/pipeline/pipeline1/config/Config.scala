package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config

import simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config.ConfigStore._
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  Subgraph_1: simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.graph.Subgraph_1.config.Config =
    simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.graph.Subgraph_1.config
      .Config()
) extends ConfigBase
