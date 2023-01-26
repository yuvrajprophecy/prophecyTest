package abhishek0005prophecy.io_team.rohitmigrationtesting.pipeline.pipeline1.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
