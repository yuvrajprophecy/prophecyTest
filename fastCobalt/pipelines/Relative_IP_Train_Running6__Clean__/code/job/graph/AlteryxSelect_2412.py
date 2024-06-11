from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2412(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("MBR_GNDR_CD"), 
        col("MBR_SK"), 
        col("MEMBER_AGE"), 
        col("MBR_INDV_BE_KEY"), 
        col("MEMBER_AGE2"), 
        col("TOT_MONTHS")
    )
