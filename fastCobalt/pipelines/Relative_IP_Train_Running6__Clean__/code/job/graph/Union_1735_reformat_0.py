from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_1735_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MEMBER_AGE2"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_SK"), 
        col("MEMBER_AGE"), 
        col("First_IP").cast(StringType()).alias("First_IP"), 
        col("IP").cast(DoubleType()).alias("IP"), 
        col("MBR_GNDR_CD"), 
        col("IP_Record").cast(StringType()).alias("IP_Record"), 
        lit(None).cast(StringType()).alias("TOT_MONTHS")
    )
