from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2318(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "IP_Running6",
          (((((col("IP") + col("IP_lag1")) + col("IP_lag2")) + col("IP_lag3")) + col("IP_lag4")) + col("IP_lag5"))
        )\
        .drop("IP_lag1")\
        .drop("IP_lag2")\
        .drop("IP_lag3")\
        .drop("IP_lag4")\
        .drop("IP_lag5")
