from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_727(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Max_YMD", call_spark_fcn("string_substring", add_months(col("Max_YMD"), 1), lit(0), lit(10)))\
        .withColumn("Max_YEARMONTH", call_spark_fcn("string_substring", col("Max_YMD"), lit(0), lit(7)))
