package io.prophecy.pipelines.frfrf.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
