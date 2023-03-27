package basetest.pipeline9.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
