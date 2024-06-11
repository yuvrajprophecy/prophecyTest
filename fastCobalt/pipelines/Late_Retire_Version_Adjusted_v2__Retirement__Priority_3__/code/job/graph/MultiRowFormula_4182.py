from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_4182(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Target Forecasted",
          when(
              (
                ((((((((length(col("YMD_lead1")).cast(IntegerType()) > lit(0)) | (length(col("YMD_lead2")).cast(IntegerType()) > lit(0))) | (length(col("YMD_lead3")).cast(IntegerType()) > lit(0))) | (length(col("YMD_lead4")).cast(IntegerType()) > lit(0))) | (length(col("YMD_lead5")).cast(IntegerType()) > lit(0))) | (length(col("YMD_lead6")).cast(IntegerType()) > lit(0))) | (length(col("YMD_lead7")).cast(IntegerType()) > lit(0))) | (length(col("YMD_lead8")).cast(IntegerType()) > lit(0)))
                | (length(col("YMD_lead9")).cast(IntegerType()) > lit(0))
              ), 
              lit(2)
            )\
            .when(
              (
                ((length(col("YMD_lead12")).cast(IntegerType()) > lit(0)) | (length(col("YMD_lead11")).cast(IntegerType()) > lit(0)))
                | (length(col("YMD_lead10")).cast(IntegerType()) > lit(0))
              ), 
              lit(1)
            )\
            .otherwise(lit(0))
        )\
        .drop("YMD_lead1")\
        .drop("YMD_lead2")\
        .drop("YMD_lead3")\
        .drop("YMD_lead4")\
        .drop("YMD_lead5")\
        .drop("YMD_lead6")\
        .drop("YMD_lead7")\
        .drop("YMD_lead8")\
        .drop("YMD_lead9")\
        .drop("YMD_lead12")\
        .drop("YMD_lead11")\
        .drop("YMD_lead10")
