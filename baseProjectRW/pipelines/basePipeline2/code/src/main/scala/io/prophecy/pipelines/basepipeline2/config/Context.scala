package io.prophecy.pipelines.basepipeline2.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
