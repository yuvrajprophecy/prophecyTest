from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2896(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "mover indicator",
          when((col("ADDR_COMPLETE_APT_FIXED_lag1") != col("ADDR_COMPLETE_APT_FIXED")), lit(1)).otherwise(lit(0))
        )\
        .drop("ADDR_COMPLETE_APT_FIXED_lag1")
