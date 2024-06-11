from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_1883(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("IP"), 
        col("MBR_SK"), 
        col("Relative_IP_Month"), 
        col("First_IP"), 
        col("MEMBER_AGE2"), 
        col("IP_Record"), 
        col("IP_Month_Number"), 
        col("MBR_INDV_BE_KEY"), 
        col("YEARMONTH"), 
        col("MEMBER_AGE"), 
        col("MBR_GNDR_CD"), 
        col("TOT_MONTHS")
    )
