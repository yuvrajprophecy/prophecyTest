from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_15(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("Call/hr", (col("Sum_Sum_Sum_TTLCalls") / col("Sum_Sum_Sum_TTLHours")))
