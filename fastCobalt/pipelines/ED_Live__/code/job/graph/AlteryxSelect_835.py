from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_835(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("DIAG_CD"), 
        col("CLM_SVC_STRT_DT_SK"), 
        col("MBR_UNIQ_KEY"), 
        col("DIAG_CD_1_SK"), 
        col("DIAG_CD_2_SK"), 
        col("DIAG_CD_3_SK")
    )
