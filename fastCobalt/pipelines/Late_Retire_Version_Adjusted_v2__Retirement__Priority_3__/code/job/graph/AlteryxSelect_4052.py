from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_4052(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("DIAG_CD_3_SK"), 
        col("SRC_SYS_CD"), 
        col("`Claim Type lvl 2`").alias("Claim Type lvl 2"), 
        col("PROC_CD_3_SK"), 
        col("DIAG_CD_1_SK"), 
        col("PROC_CD_2_SK"), 
        col("`Service Date`").alias("Service Date"), 
        col("FUND_CAT_CD"), 
        col("PROC_CD_1_SK"), 
        col("CLM_LN_POS_CD"), 
        col("CLM_LN_PROC_CD_SK"), 
        col("`Dental Category Code`").alias("Dental Category Code"), 
        col("`Claim Type lvl 1`").alias("Claim Type lvl 1"), 
        col("DIAG_CD_2_SK"), 
        col("DIAG_CD_1_SK2"), 
        col("`Received Date`").alias("Received Date"), 
        col("`Diagnosis Type`").alias("Diagnosis Type"), 
        col("NDC_SK"), 
        col("CLM_FINL_DISP_CD"), 
        col("SRC_SYS_CD2"), 
        col("DRUG_CLM_TIER_CD"), 
        col("MBR_INDV_BE_KEY"), 
        col("SpecialtyIndicator"), 
        col("GenericIndicator"), 
        col("MailOrder"), 
        col("MemberPay"), 
        col("IngredientCost"), 
        col("RetailCost")
    )
