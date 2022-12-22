package simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.config

import simpledatalabs_27.applicationprojectext2.pipeline.pipeline1.config.ConfigStore._
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  Subgraph_1: simpledatalabs_27.applicationprojectext2.subgraph.appsg1.config.Config =
    simpledatalabs_27.applicationprojectext2.subgraph.appsg1.config.Config()
) extends ConfigBase
