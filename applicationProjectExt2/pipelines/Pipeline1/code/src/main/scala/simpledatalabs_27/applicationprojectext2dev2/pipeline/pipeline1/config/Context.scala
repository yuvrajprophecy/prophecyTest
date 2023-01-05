package simpledatalabs_27.applicationprojectext2dev2.pipeline.pipeline1.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
