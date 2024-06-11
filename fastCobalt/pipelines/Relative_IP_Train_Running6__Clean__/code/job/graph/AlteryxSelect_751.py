from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_751(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("OfLast6_Main_Rx"), 
        col("YEARMONTH"), 
        col("Primary_Diagnosis"), 
        col("Primary_Rx_Running3"), 
        col("OfLast6_Main_Rx_Costs"), 
        col("Primary_RxCosts_Running3"), 
        col("Tertiary_Rx_Running3"), 
        col("MBR_SK"), 
        col("Secondary_RxCosts_Running3"), 
        col("Secondary_Rx_Running3"), 
        col("MBR_INDV_BE_KEY"), 
        col("Tertiary_RxCosts_Running3")
    )
