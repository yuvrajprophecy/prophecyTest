package simpledatalabs_27.rohitmigrationtesting.pipeline.pipeline1.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
