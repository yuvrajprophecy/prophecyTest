from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_4062(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("yr_mo", call_spark_fcn("string_substring", col("`Service Date`"), lit(0), lit(7)))\
        .withColumn("Winterizing Procedure", lit(None))
