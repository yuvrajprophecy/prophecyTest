from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_1948(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("YEARMONTH"), col("MBR_INDV_BE_KEY"), col("First_IP"), col("Counter"))
