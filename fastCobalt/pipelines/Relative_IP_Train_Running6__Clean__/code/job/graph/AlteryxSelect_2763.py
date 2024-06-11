from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2763(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("HighestAlbumin_Running3"), 
        col("CountOfLabs_Running3"), 
        col("MBR_INDV_BE_KEY"), 
        col("UniqueLabs_Running3"), 
        col("Concat_ORDER_TST_NM_Running3"), 
        col("LowestGFR_Running3")
    )
