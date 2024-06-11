from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_738(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("PRI_NDC_BRND_NM"), 
        col("DIAG_CD_DESC"), 
        col("MBR_SK"), 
        col("NDC"), 
        col("MBR_INDV_BE_KEY"), 
        col("NDC_COST"), 
        col("PRI_NDC_SK"), 
        col("ROW_NO")
    )
