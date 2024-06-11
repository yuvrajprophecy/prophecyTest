from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2319(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("IP_Running6_Max"), 
        col("IP_Running6"), 
        col("IP"), 
        col("IP_PreviousMonth"), 
        col("MBR_SK"), 
        col("IP_Running3_Max"), 
        col("IP_Running3")
    )
