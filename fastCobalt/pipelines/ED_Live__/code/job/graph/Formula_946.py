from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_946(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "YMD",
        concat(call_spark_fcn("string_substring", col("CLM_INPT_DT_SK"), lit(0), lit(7)), lit("-01"))
    )
