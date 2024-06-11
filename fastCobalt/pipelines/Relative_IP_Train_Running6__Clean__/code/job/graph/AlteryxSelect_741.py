from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_741(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("DIAG_CD_DESC").alias("Primary_Diagnosis"), 
        col("MBR_SK"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("NDC_COST").alias("Primary_RX_Cost"), 
        col("NDC").alias("Primary_RX")
    )
