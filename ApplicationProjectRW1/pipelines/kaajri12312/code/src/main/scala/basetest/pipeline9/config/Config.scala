package basetest.pipeline9.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import basetest.pipeline9.graph.sdfsd.config.{Config => sdfsd_Config}
case class Config(sdfsd: sdfsd_Config = sdfsd_Config()) extends ConfigBase
