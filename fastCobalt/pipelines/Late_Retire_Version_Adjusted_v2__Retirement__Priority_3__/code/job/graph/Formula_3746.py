from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_3746(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Apt Ind", (col("MBR_HOME_ADDR_LN_1") + col("MBR_HOME_ADDR_LN_2")))\
        .withColumn(
          "Apt Ind",
          when(
              (
                (col("`Apt Ind`").contains(lit("APT")) | col("`Apt Ind`").contains(lit("unit")))
                | col("`Apt Ind`").contains(lit("apartment"))
              ), 
              lit(1)
            )\
            .otherwise(lit(0))
        )\
        .withColumn(
          "Male Percentage",
          when(
              ~ col("`Male Percentage`").isNull(), 
              (
                round(
                    (
                      (col("`Male Percentage`").cast(IntegerType()).cast(IntegerType()) * lit(100)).cast(DoubleType())
                      * lit(0.1)
                    )
                  )\
                  .cast(DoubleType())
                / lit(0.1)
              )
            )\
            .otherwise(lit(None))
        )\
        .withColumn(
          "Female Percentage",
          when(
              ~ col("`Female Percentage`").isNull(), 
              (
                round(
                    (
                      (col("`Female Percentage`").cast(IntegerType()).cast(IntegerType()) * lit(100)).cast(DoubleType())
                      * lit(0.1)
                    )
                  )\
                  .cast(DoubleType())
                / lit(0.1)
              )
            )\
            .otherwise(lit(None))
        )\
        .withColumn(
          "Average Age Males",
          when(
              ~ col("`Average Age Males`").isNull(), 
              (
                round((col("`Average Age Males`").cast(IntegerType()).cast(DoubleType()) * lit(0.2))).cast(DoubleType())
                / lit(0.2)
              )
            )\
            .otherwise(lit(None))
        )\
        .withColumn(
          "Average Age Females",
          when(
              ~ col("`Average Age Females`").isNull(), 
              (
                round((col("`Average Age Females`").cast(IntegerType()).cast(DoubleType()) * lit(0.2)))\
                  .cast(DoubleType())
                / lit(0.2)
              )
            )\
            .otherwise(lit(None))
        )\
        .withColumn(
          "Member Months",
          when(
              ~ col("`Member Months`").isNull(), 
              (
                round(
                    (
                      (col("`Member Months`").cast(IntegerType()).cast(IntegerType()) / lit(12)).cast(IntegerType())
                      * lit(2)
                    )
                  )\
                  .cast(IntegerType())
                / lit(2)
              )
            )\
            .otherwise(lit(None))
        )\
        .withColumn("Group Months", when(
          ~ col("`Group Months`").isNull(), 
          (
            round(((col("`Group Months`").cast(IntegerType()).cast(IntegerType()) / lit(12)).cast(IntegerType()) * lit(2)))\
              .cast(IntegerType())
            / lit(2)
          )
        )\
        .otherwise(lit(None)))
