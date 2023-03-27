package io.prophecy.pipelines.lineageregression.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
