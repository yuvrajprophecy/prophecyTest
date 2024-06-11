from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_935(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Prediction Rounded",
          (round((col("positive_probability").cast(IntegerType()) * lit(10))).cast(IntegerType()) / lit(10))
        )\
        .withColumn("Buckets", when((col("positive_probability").cast(DoubleType()) < lit(0.3)), lit("2. Medium"))\
        .when((col("positive_probability").cast(DoubleType()) < lit(0.45)), lit("1. High"))\
        .otherwise(lit("0. Very High")))
