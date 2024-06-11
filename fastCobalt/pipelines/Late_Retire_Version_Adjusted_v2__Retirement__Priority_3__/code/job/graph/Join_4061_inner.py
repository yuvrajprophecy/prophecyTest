from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4061_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.CLM_LN_PROC_CD_SK") == col("in1.PROC_CD_SK")), "inner")\
        .select(col("in0.PROC_CD_2_SK").alias("PROC_CD_2_SK"), col("in0.IngredientCost").alias("IngredientCost"), col("in0.MemberPay").alias("MemberPay"), col("in0.DIAG_CD_2_SK").alias("DIAG_CD_2_SK"), col("in0.PROC_CD_3_SK").alias("PROC_CD_3_SK"), col("in0.DIAG_CD_3_SK").alias("DIAG_CD_3_SK"), col("in0.SRC_SYS_CD").alias("SRC_SYS_CD"), col("in0.CLM_LN_POS_CD").alias("CLM_LN_POS_CD"), col("in0.CLM_LN_PROC_CD_SK").alias("CLM_LN_PROC_CD_SK"), col("in0.`Claim Type lvl 1`").alias("Claim Type lvl 1"), col("in0.`Dental Category Code`").alias("Dental Category Code"), col("in1.`Procedure Type`").alias("Procedure Type"), col("in1.PROC_CD_TERM_DT_SK").alias("PROC_CD_TERM_DT_SK"), col("in0.DRUG_CLM_TIER_CD").alias("DRUG_CLM_TIER_CD"), col("in0.SRC_SYS_CD2").alias("SRC_SYS_CD2"), col("in0.`Diagnosis Type`").alias("Diagnosis Type"), col("in0.SpecialtyIndicator").alias("SpecialtyIndicator"), col("in0.CLM_FINL_DISP_CD").alias("CLM_FINL_DISP_CD"), col("in0.MailOrder").alias("MailOrder"), col("in0.NDC_SK").alias("NDC_SK"), col("in0.DIAG_CD_1_SK2").alias("DIAG_CD_1_SK2"), col("in0.PROC_CD_1_SK").alias("PROC_CD_1_SK"), col("in0.`Service Date`").alias("Service Date"), col("in0.FUND_CAT_CD").alias("FUND_CAT_CD"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.`Received Date`").alias("Received Date"), col("in0.counter").alias("counter"), col("in0.RetailCost").alias("RetailCost"), col("in0.DIAG_CD_1_SK").alias("DIAG_CD_1_SK"), col("in0.GenericIndicator").alias("GenericIndicator"), col("in0.`Claim Type lvl 2`").alias("Claim Type lvl 2"))
