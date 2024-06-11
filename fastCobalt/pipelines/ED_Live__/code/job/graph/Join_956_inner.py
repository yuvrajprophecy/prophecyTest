from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_956_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Diagnosis") == col("in1.Diagnosis")), "inner")\
        .select(col("in0.Avg_edcnpa").alias("Avg_edcnpa"), col("in0.Diagnosis").alias("Diagnosis"), col("in0.Avg_edcnnpa").alias("Avg_edcnnpa"), col("in0.Avg_alcohol").alias("Avg_alcohol"), col("in0.Avg_psych").alias("Avg_psych"), col("in1.DIAG_CD").alias("DIAG_CD"), col("in0.Avg_noner").alias("Avg_noner"), col("in0.Avg_epct").alias("Avg_epct"), col("in1.YMD").alias("YMD"), col("in1.CLM_LN_TOT_ALW_AMT").alias("CLM_LN_TOT_ALW_AMT"), col("in1.DIAG_CD_DESC").alias("DIAG_CD_DESC"), col("in1.EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD"), col("in0.Avg_injury").alias("Avg_injury"), col("in0.Avg_drug").alias("Avg_drug"), col("in1.SUB_ID").alias("SUB_ID"), col("in1.icd10").alias("icd10"), col("in1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in1.RVNU_CD").alias("RVNU_CD"), col("in1.CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"), col("in1.`ED Visits`").alias("ED Visits"), col("in1.CLM_INPT_DT_SK").alias("CLM_INPT_DT_SK"), col("in1.CLM_ID").alias("CLM_ID"), col("in1.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"))
