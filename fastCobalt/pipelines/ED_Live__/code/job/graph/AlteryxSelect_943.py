from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_943(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("EXP_SUB_CAT_CD"), 
        col("DIAG_CD"), 
        col("icd10"), 
        col("YMD"), 
        col("CLM_INPT_DT_SK"), 
        col("CLM_LN_ALW_AMT"), 
        col("RVNU_CD"), 
        col("DIAG_CD_DESC"), 
        col("SUB_ID"), 
        col("CLM_SVC_STRT_DT_SK"), 
        col("CLM_LN_TOT_ALW_AMT"), 
        col("CLM_ID"), 
        col("MBR_INDV_BE_KEY"), 
        col("`ED Visits`").alias("ED Visits")
    )
