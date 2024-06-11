from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_2751(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("albumin_test", when(col("LAB_RSLT_DESC").contains(lit("albumin")), lit(1)).otherwise(lit(0)))\
        .withColumn("GFR/EGFR", when(col("LAB_RSLT_DESC").contains(lit("gfr")), lit(1)).otherwise(lit(0)))\
        .withColumn("LAB_RSLT_SVC_DT_SK", call_spark_fcn("string_substring", col("LAB_RSLT_SVC_DT_SK"), lit(0), lit(7)))
