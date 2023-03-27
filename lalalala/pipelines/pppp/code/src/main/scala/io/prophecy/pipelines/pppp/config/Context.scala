package io.prophecy.pipelines.pppp.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
