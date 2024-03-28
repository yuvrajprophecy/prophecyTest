package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  @Description("source path") pSourcePath:                String = "",
  @Description("source file") pSourceFile:                String = "",
  @Description("source schema path") pSourceSchemaPath:   String = "",
  @Description("source schema file") pSourceSchemaFile:   String = "",
  @Description("target dataset name") pTargetDatasetName: String = "",
  @Description("target path") pTargetPath:                String = "",
  @Description("Current Date time") DATE:                 String = "1711610061900"
) extends ConfigBase
