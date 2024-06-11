from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Transpose_4044_schemaTransform(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.drop("Service Date")
    df2 = df1.drop("NDC_SK")
    df3 = df2.drop("CLM_LN_PROC_CD_SK")
    df4 = df3.drop("DIAG_CD_1_SK2")
    df5 = df4.drop("Claim Type lvl 1")
    df6 = df5.drop("Claim Type lvl 2")
    df7 = df6.drop("Dental Category Code")
    df8 = df7.drop("Diagnosis Type")
    df9 = df8.drop("CLM_LN_POS_CD")
    df10 = df9.drop("SRC_SYS_CD")
    df11 = df10.drop("FUND_CAT_CD")
    df12 = df11.drop("SRC_SYS_CD2")
    df13 = df12.drop("CLM_FINL_DISP_CD")
    df14 = df13.drop("DRUG_CLM_TIER_CD")
    df15 = df14.drop("SpecialtyIndicator")
    df16 = df15.drop("GenericIndicator")
    df17 = df16.drop("MailOrder")
    df18 = df17.drop("MemberPay")
    df19 = df18.drop("IngredientCost")

    return df19.drop("RetailCost")
