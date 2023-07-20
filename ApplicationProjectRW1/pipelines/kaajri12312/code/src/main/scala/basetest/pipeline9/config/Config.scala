package basetest.pipeline9.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
case class Config(c1: String = "dd") extends ConfigBase
