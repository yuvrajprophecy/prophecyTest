from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_400(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Diagnosis"), 
        col("EXP_SUB_CAT_CD"), 
        col("PROV_ID"), 
        col("DIAG_CD"), 
        col("Week"), 
        col("icd10"), 
        col("YMD"), 
        col("CLM_INPT_DT_SK"), 
        col("CLM_LN_ALW_AMT"), 
        col("RVNU_CD"), 
        col("MBR_INDV_BE_KEY"), 
        col("SUB_ID"), 
        col("CLM_SVC_STRT_DT_SK"), 
        col("CLM_LN_TOT_ALW_AMT"), 
        col("CLM_ID"), 
        col("Avg_edcnnpa").alias("edcnnpa"), 
        col("Avg_edcnpa").alias("edcnpa"), 
        col("Avg_epct").alias("epct"), 
        col("Avg_noner").alias("noner"), 
        col("Avg_injury").alias("injury"), 
        col("Avg_psych").alias("psych"), 
        col("Avg_alcohol").alias("alcohol"), 
        col("Avg_drug").alias("drug")
    )
