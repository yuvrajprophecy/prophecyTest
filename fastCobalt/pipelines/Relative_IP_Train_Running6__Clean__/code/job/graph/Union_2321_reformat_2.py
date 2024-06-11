from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_2321_reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MEMBER_AGE2"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_SK"), 
        col("MEMBER_AGE"), 
        col("MBR_GNDR_CD"), 
        col("TOT_MONTHS"), 
        lit(None).cast(IntegerType()).alias("IP_Running6"), 
        lit(None).cast(StringType()).alias("Relative_IP_Month"), 
        lit(None).cast(IntegerType()).alias("IP_Running3"), 
        lit(None).cast(IntegerType()).alias("IP_Running3_Max"), 
        lit(None).cast(StringType()).alias("First_IP"), 
        lit(None).cast(IntegerType()).alias("IP_Running6_Max"), 
        lit(None).cast(IntegerType()).alias("IP_PreviousMonth"), 
        lit(None).cast(IntegerType()).alias("IP_Month_Number"), 
        lit(None).cast(StringType()).alias("IP"), 
        lit(None).cast(IntegerType()).alias("RecordID"), 
        lit(None).cast(StringType()).alias("IP_Record")
    )
