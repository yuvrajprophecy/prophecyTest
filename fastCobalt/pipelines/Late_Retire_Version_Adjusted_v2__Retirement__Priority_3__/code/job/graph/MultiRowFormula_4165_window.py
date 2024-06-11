from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_4165_window(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Switch to Retirement_lead12",
          lead(col("`Switch to Retirement`"), 12)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead11",
          lead(col("`Switch to Retirement`"), 11)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead10",
          lead(col("`Switch to Retirement`"), 10)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead1",
          lead(col("`Switch to Retirement`"), 1)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead2",
          lead(col("`Switch to Retirement`"), 2)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead3",
          lead(col("`Switch to Retirement`"), 3)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead4",
          lead(col("`Switch to Retirement`"), 4)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead5",
          lead(col("`Switch to Retirement`"), 5)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead6",
          lead(col("`Switch to Retirement`"), 6)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead7",
          lead(col("`Switch to Retirement`"), 7)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Switch to Retirement_lead8",
          lead(col("`Switch to Retirement`"), 8)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn("Switch to Retirement_lead9", lead(col("`Switch to Retirement`"), 9)\
        .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc())))
