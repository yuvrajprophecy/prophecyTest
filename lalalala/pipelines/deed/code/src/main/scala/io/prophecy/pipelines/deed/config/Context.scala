package io.prophecy.pipelines.deed.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
