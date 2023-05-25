package io.prophecy.pipelines.pipeline111.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
