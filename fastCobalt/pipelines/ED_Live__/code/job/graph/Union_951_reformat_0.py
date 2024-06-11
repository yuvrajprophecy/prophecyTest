from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_951_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("noner"), 
        col("edcnnpa"), 
        col("MBR_INDV_BE_KEY"), 
        col("SUB_ID"), 
        col("YMD"), 
        col("CLM_LN_ALW_AMT"), 
        col("CLM_ID"), 
        col("EXP_SUB_CAT_CD"), 
        col("injury"), 
        col("DIAG_CD_DESC"), 
        col("psych"), 
        col("CLM_LN_TOT_ALW_AMT"), 
        col("epct"), 
        col("`ED Visits`").alias("ED Visits"), 
        col("edcnpa"), 
        col("DIAG_CD"), 
        col("icd10"), 
        col("Diagnosis").cast(StringType()).alias("Diagnosis"), 
        col("CLM_SVC_STRT_DT_SK"), 
        col("RVNU_CD"), 
        col("CLM_INPT_DT_SK"), 
        col("alcohol"), 
        col("drug")
    )
