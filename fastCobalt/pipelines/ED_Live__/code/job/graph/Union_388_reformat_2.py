from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_388_reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("SUB_ID"), 
        col("YMD"), 
        col("CLM_LN_ALW_AMT"), 
        col("CLM_ID"), 
        col("EXP_SUB_CAT_CD"), 
        col("CLM_LN_TOT_ALW_AMT"), 
        col("DIAG_CD"), 
        col("Week"), 
        col("icd10"), 
        col("Diagnosis").cast(StringType()).alias("Diagnosis"), 
        col("CLM_SVC_STRT_DT_SK"), 
        col("RVNU_CD"), 
        col("CLM_INPT_DT_SK"), 
        col("PROV_ID"), 
        lit(None).cast(StringType()).alias("noner"), 
        lit(None).cast(StringType()).alias("edcnnpa"), 
        lit(None).cast(StringType()).alias("injury"), 
        lit(None).cast(StringType()).alias("psych"), 
        lit(None).cast(StringType()).alias("epct"), 
        lit(None).cast(StringType()).alias("edcnpa"), 
        lit(None).cast(StringType()).alias("alcohol"), 
        lit(None).cast(StringType()).alias("drug")
    )
