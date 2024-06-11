from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_31_window(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Percent of Calls_lag1",
          lag(col("`Percent of Calls`"), 1)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag2",
          lag(col("`Percent of Calls`"), 2)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag3",
          lag(col("`Percent of Calls`"), 3)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag4",
          lag(col("`Percent of Calls`"), 4)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag5",
          lag(col("`Percent of Calls`"), 5)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag1",
          lag(col("`Percent of Calls`"), 1)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag7",
          lag(col("`Percent of Calls`"), 7)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag8",
          lag(col("`Percent of Calls`"), 8)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag9",
          lag(col("`Percent of Calls`"), 9)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag10",
          lag(col("`Percent of Calls`"), 10)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag11",
          lag(col("`Percent of Calls`"), 11)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag12",
          lag(col("`Percent of Calls`"), 12)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag13",
          lag(col("`Percent of Calls`"), 13)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag14",
          lag(col("`Percent of Calls`"), 14)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag15",
          lag(col("`Percent of Calls`"), 15)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag16",
          lag(col("`Percent of Calls`"), 16)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag17",
          lag(col("`Percent of Calls`"), 17)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag18",
          lag(col("`Percent of Calls`"), 18)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn(
          "Percent of Calls_lag19",
          lag(col("`Percent of Calls`"), 19)\
            .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc()))
        )\
        .withColumn("Percent of Calls_lag20", lag(col("`Percent of Calls`"), 20)\
        .over(Window.partitionBy(col("`Call Type`")).orderBy(col("`Call Type`").asc())))
