package basetest.pipeline91.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
