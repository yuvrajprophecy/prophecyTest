from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_539(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("IP_Running6_Max"), 
        col("HighestAlbumin_Running3"), 
        col("CountOfLabs_Running3"), 
        col("IP_Running6"), 
        col("RecordID"), 
        col("IP_PreviousMonth"), 
        col("MBR_GNDR_CD"), 
        col("MBR_SK"), 
        col("Relative_IP_Month"), 
        col("IP_Running3_Max"), 
        col("MEMBER_AGE"), 
        col("MBR_INDV_BE_KEY"), 
        col("First_IP"), 
        col("MEMBER_AGE2"), 
        col("TOT_MONTHS"), 
        col("IP_Record"), 
        col("UniqueLabs_Running3"), 
        col("Concat_ORDER_TST_NM_Running3"), 
        col("LowestGFR_Running3"), 
        col("IP_Running3"), 
        col("IP_Month_Number")
    )
