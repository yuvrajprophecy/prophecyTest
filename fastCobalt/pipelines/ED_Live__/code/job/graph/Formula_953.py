from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_953(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("Diagnosis", call_spark_fcn("string_substring", col("icd10"), lit(0), lit(3)))
