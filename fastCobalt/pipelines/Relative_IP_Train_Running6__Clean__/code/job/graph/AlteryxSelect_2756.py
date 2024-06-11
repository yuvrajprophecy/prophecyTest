from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2756(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("GFR/EGFR"), 
        col("albumin_test"), 
        col("LowestGFR"), 
        col("MBR_INDV_BE_KEY"), 
        col("HighestAlbumin"), 
        col("mmol"), 
        col("LAB_RSLT_SVC_DT_SK").alias("YEARMONTH")
    )
