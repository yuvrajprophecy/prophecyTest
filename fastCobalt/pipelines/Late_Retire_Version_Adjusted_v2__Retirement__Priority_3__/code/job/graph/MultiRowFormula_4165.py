from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_4165(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Target Forecasted",
          when(
              (
                ((col("`Switch to Retirement_lead12`").cast(IntegerType()) == lit(1)) | (col("`Switch to Retirement_lead11`").cast(IntegerType()) == lit(1)))
                | (col("`Switch to Retirement_lead10`").cast(IntegerType()) == lit(1))
              ), 
              lit(1)
            )\
            .when(
              (
                ((((((((col("`Switch to Retirement_lead1`").cast(IntegerType()) == lit(1)) | (col("`Switch to Retirement_lead2`").cast(IntegerType()) == lit(1))) | (col("`Switch to Retirement_lead3`").cast(IntegerType()) == lit(1))) | (col("`Switch to Retirement_lead4`").cast(IntegerType()) == lit(1))) | (col("`Switch to Retirement_lead5`").cast(IntegerType()) == lit(1))) | (col("`Switch to Retirement_lead6`").cast(IntegerType()) == lit(1))) | (col("`Switch to Retirement_lead7`").cast(IntegerType()) == lit(1))) | (col("`Switch to Retirement_lead8`").cast(IntegerType()) == lit(1)))
                | (col("`Switch to Retirement_lead9`").cast(IntegerType()) == lit(1))
              ), 
              lit(2)
            )\
            .otherwise(lit(0))
        )\
        .drop("Switch to Retirement_lead12")\
        .drop("Switch to Retirement_lead11")\
        .drop("Switch to Retirement_lead10")\
        .drop("Switch to Retirement_lead1")\
        .drop("Switch to Retirement_lead2")\
        .drop("Switch to Retirement_lead3")\
        .drop("Switch to Retirement_lead4")\
        .drop("Switch to Retirement_lead5")\
        .drop("Switch to Retirement_lead6")\
        .drop("Switch to Retirement_lead7")\
        .drop("Switch to Retirement_lead8")\
        .drop("Switch to Retirement_lead9")
