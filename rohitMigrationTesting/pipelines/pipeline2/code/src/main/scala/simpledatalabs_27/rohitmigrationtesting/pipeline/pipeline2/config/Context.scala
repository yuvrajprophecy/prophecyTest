package simpledatalabs_27.rohitmigrationtesting.pipeline.pipeline2.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
