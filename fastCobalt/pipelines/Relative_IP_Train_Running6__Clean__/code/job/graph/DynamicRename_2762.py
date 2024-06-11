from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DynamicRename_2762(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("YEARMONTH"), 
        col("Concat_ORDER_TST_NM").alias("Concat_ORDER_TST_NM_Running3"), 
        col("UniqueLabs").alias("UniqueLabs_Running3"), 
        col("HighestAlbumin").alias("HighestAlbumin_Running3"), 
        col("LowestGFR").alias("LowestGFR_Running3"), 
        col("CountOfLabs").alias("CountOfLabs_Running3")
    )
