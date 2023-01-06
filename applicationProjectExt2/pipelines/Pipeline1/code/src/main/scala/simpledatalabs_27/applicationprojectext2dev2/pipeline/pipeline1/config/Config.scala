package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import simpledatalabs_27.applicationprojectext2dev2.subgraph.sg2
import simpledatalabs_27.applicationprojectext2dev2.subgraph.sg1

case class Config(
  Subgraph_1: sg1.config.Config = sg1.config.Config(),
  Subgraph_2: sg2.config.Config = sg2.config.Config()
) extends ConfigBase
