from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2856(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_GNDR_CD"), 
        col("ACTVTY_YR_MO_SK"), 
        col("MBR_AGE_AT_ACTVTY_YR_MO"), 
        col("GRP_NM"), 
        col("MBR_INDV_BE_KEY"), 
        col("GRP_ID")
    )
