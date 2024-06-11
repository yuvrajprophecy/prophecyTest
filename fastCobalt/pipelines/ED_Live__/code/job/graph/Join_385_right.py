from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_385_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in1.icd10") == col("in0.DIAG_CD")), "leftanti")\
        .select(col("in0.CLM_INPT_DT_SK").alias("CLM_INPT_DT_SK"), col("in0.CLM_ID").alias("CLM_ID"), col("in0.EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD"), col("in0.PROV_ID").alias("PROV_ID"), col("in0.icd10").alias("icd10"), col("in0.RVNU_CD").alias("RVNU_CD"), col("in0.YMD").alias("YMD"), col("in0.Week").alias("Week"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"), col("in0.CLM_LN_TOT_ALW_AMT").alias("CLM_LN_TOT_ALW_AMT"), col("in0.CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"), col("in0.DIAG_CD").alias("DIAG_CD"), col("in0.SUB_ID").alias("SUB_ID"))
