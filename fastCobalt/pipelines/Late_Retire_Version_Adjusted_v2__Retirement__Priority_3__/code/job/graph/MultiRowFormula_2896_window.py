from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2896_window(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "ADDR_COMPLETE_APT_FIXED_lag1",
        lag(col("ADDR_COMPLETE_APT_FIXED"), 1)\
          .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
    )
