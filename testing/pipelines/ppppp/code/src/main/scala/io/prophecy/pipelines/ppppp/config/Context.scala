package io.prophecy.pipelines.ppppp.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
