from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_2526_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("`Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS`")\
          .cast(DoubleType())\
          .alias("Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS"), 
        col("`Female Percentage`").alias("Female Percentage"), 
        col("`Medical FACTORS_INFLUENCING_HEALTH_STATUS`")\
          .cast(DoubleType())\
          .alias("Medical FACTORS_INFLUENCING_HEALTH_STATUS"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_RELSHP_NM"), 
        col("`Average Age Males`").alias("Average Age Males"), 
        col("`Target Forecasted`").alias("Target Forecasted"), 
        col("Avg_PCB"), 
        col("Avg_BCARE"), 
        col("`Vision Member Pay`").cast(DoubleType()).alias("Vision Member Pay"), 
        col("MBR_SK"), 
        col("Avg_HPEXTRNL"), 
        col("`Unkown Drug Tier`").cast(DoubleType()).alias("Unkown Drug Tier"), 
        col("`Medical BURNS`").cast(DoubleType()).alias("Medical BURNS"), 
        col("YMD"), 
        col("`Retired With Blue`").alias("Retired With Blue"), 
        col("`Drug Tier 3`").cast(DoubleType()).alias("Drug Tier 3"), 
        col("`Male Percentage`").alias("Male Percentage"), 
        col("`Marketing Restricted`").alias("Marketing Restricted"), 
        col("`Medical RESPIRATORY_SYSTEM`").cast(DoubleType()).alias("Medical RESPIRATORY_SYSTEM"), 
        col("`Med Member Pay`").cast(DoubleType()).alias("Med Member Pay"), 
        col("Avg_BLUESELECT_"), 
        col("SUB_MBR_SK"), 
        col("`Switch to Retirement`").alias("Switch to Retirement"), 
        col("`Dental Received Date`").cast(StringType()).alias("Dental Received Date"), 
        col("`Spouse Age`").alias("Spouse Age"), 
        col("`Medical FEMALE_REPRODUCTIVE_SYSTEM`").cast(DoubleType()).alias("Medical FEMALE_REPRODUCTIVE_SYSTEM"), 
        col("MBR_FIRST_NM"), 
        col("`Dental EYE`").cast(DoubleType()).alias("Dental EYE"), 
        col("`Medical MALE_REPRODUCTIVE_SYSTEM`").cast(DoubleType()).alias("Medical MALE_REPRODUCTIVE_SYSTEM"), 
        col("MBR_HOME_ADDR_LN_2"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("`Medical NOT_APPLICABLE`").cast(DoubleType()).alias("Medical NOT_APPLICABLE"), 
        col("`Recent Mover`").alias("Recent Mover"), 
        col("PCP_FLAG"), 
        col("`Medical AIDS_HIV`").cast(DoubleType()).alias("Medical AIDS_HIV"), 
        col("`Spouse's Product`").alias("Spouse's Product"), 
        col("`Min_Dependent Age`").alias("Min_Dependent Age"), 
        col("`Medical MENTAL_ILLNESS`").cast(DoubleType()).alias("Medical MENTAL_ILLNESS"), 
        col("`Medical KIDNEY_AND_URINARY_TRACT`").cast(DoubleType()).alias("Medical KIDNEY_AND_URINARY_TRACT"), 
        col("`Medical MYELOPROLIFERATIVE_AND_NEOPLASMS`")\
          .cast(DoubleType())\
          .alias("Medical MYELOPROLIFERATIVE_AND_NEOPLASMS"), 
        col("`Dental MENTAL_ILLNESS`").cast(DoubleType()).alias("Dental MENTAL_ILLNESS"), 
        col("`Medical EAR__NOSE__AND_THROAT`").cast(DoubleType()).alias("Medical EAR__NOSE__AND_THROAT"), 
        col("`Medical BLOOD_AND_BLOOD_FORMING_ORGANS`")\
          .cast(DoubleType())\
          .alias("Medical BLOOD_AND_BLOOD_FORMING_ORGANS"), 
        col("GRP_NM"), 
        col("GRP_ID"), 
        col("GRP_BILL_LVL_NM"), 
        col("`Drug Tier 2`").cast(DoubleType()).alias("Drug Tier 2"), 
        col("`Medical Received Date`").cast(StringType()).alias("Medical Received Date"), 
        col("Avg_PC"), 
        col("`Generic Drug Counts`").cast(DoubleType()).alias("Generic Drug Counts"), 
        col("HOST_MBR_IN"), 
        col("AGE"), 
        col("MBR_DSBLTY_IN"), 
        col("EXPRNC_CAT_CD"), 
        col("FUND_CAT_CD"), 
        col("`Dental EAR__NOSE__AND_THROAT`").cast(DoubleType()).alias("Dental EAR__NOSE__AND_THROAT"), 
        col("`Medical NO_DIAGNOSTIC_CATEGORY`").cast(DoubleType()).alias("Medical NO_DIAGNOSTIC_CATEGORY"), 
        col("`Specialty Drug Counts`").cast(DoubleType()).alias("Specialty Drug Counts"), 
        col("CLS_PLN_DESC"), 
        col("`Medical DIGESTIVE_SYSTEM`").cast(DoubleType()).alias("Medical DIGESTIVE_SYSTEM"), 
        col("PROD_SH_NM_DLVRY_METH_CD"), 
        col("Members_on_Policy"), 
        col("SUM_CCI"), 
        col("`Retiree in Household Indicator`").alias("Retiree in Household Indicator"), 
        col("PROD_SH_NM"), 
        col("`Dental NO_DIAGNOSTIC_CATEGORY`").cast(DoubleType()).alias("Dental NO_DIAGNOSTIC_CATEGORY"), 
        col("GRP_TOT_EMPL_CT"), 
        col("`Medical INFECTIOUS_AND_PARASITIC_DISEASES`")\
          .cast(DoubleType())\
          .alias("Medical INFECTIOUS_AND_PARASITIC_DISEASES"), 
        col("`RX Member Pay`").cast(DoubleType()).alias("RX Member Pay"), 
        col("`Drug Tier 1`").cast(DoubleType()).alias("Drug Tier 1"), 
        col("`Dental FACTORS_INFLUENCING_HEALTH_STATUS`")\
          .cast(DoubleType())\
          .alias("Dental FACTORS_INFLUENCING_HEALTH_STATUS"), 
        col("`Vision EYE`").cast(DoubleType()).alias("Vision EYE"), 
        col("`Medical UNKNOWN`").cast(DoubleType()).alias("Medical UNKNOWN"), 
        col("SPIRA_BNF_ID"), 
        col("MBR_GNDR_CD"), 
        col("`Dental Dental Member Pay`").cast(DoubleType()).alias("Dental Dental Member Pay"), 
        col("`Member Months`").alias("Member Months"), 
        col("MBR_ENR_COBRA_IN"), 
        col("MBR_HOME_ADDR_LN_1"), 
        col("`Vision FACTORS_INFLUENCING_HEALTH_STATUS`")\
          .cast(DoubleType())\
          .alias("Vision FACTORS_INFLUENCING_HEALTH_STATUS"), 
        col("`Distinct NDC Codes`").cast(StringType()).alias("Distinct NDC Codes"), 
        col("`Dental NOT_APPLICABLE`").cast(DoubleType()).alias("Dental NOT_APPLICABLE"), 
        col("`Dental ALCOHOL_DRUG_USE_AND_DISORDERS`").cast(DoubleType()).alias("Dental ALCOHOL_DRUG_USE_AND_DISORDERS"), 
        col("`Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC`")\
          .cast(DoubleType())\
          .alias("Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), 
        col("`Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`")\
          .cast(DoubleType())\
          .alias("Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), 
        col("`Average Age Females`").alias("Average Age Females"), 
        col("`Product Category`").alias("Product Category"), 
        col("`Distinct Diagnosis Types`").cast(StringType()).alias("Distinct Diagnosis Types"), 
        col("`Medical HEPATOBILARY_SYSTEM`").cast(DoubleType()).alias("Medical HEPATOBILARY_SYSTEM"), 
        col("Avg_BLUE_SELECT"), 
        col("`Mail Order Drug Counts`").cast(DoubleType()).alias("Mail Order Drug Counts"), 
        col("GRP_MKT_SIZE_CAT_NM"), 
        col("MBR_LAST_NM"), 
        col("Product"), 
        col("`Group Months`").alias("Group Months"), 
        col("`Vision NO_DIAGNOSTIC_CATEGORY`").cast(DoubleType()).alias("Vision NO_DIAGNOSTIC_CATEGORY"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("`Dental SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`")\
          .cast(DoubleType())\
          .alias("Dental SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), 
        col("`Medical EYE`").cast(DoubleType()).alias("Medical EYE"), 
        col("MBR_HOME_ADDR_ST_CD"), 
        col("FNCL_LOB_CD"), 
        col("FNCL_MKT_SEG_NM"), 
        col("`Medical ALCOHOL_DRUG_USE_AND_DISORDERS`")\
          .cast(DoubleType())\
          .alias("Medical ALCOHOL_DRUG_USE_AND_DISORDERS"), 
        col("`Medical MUSCULOSKETAL_SYSTEM`").cast(DoubleType()).alias("Medical MUSCULOSKETAL_SYSTEM"), 
        col("`Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC`")\
          .cast(DoubleType())\
          .alias("Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), 
        col("Retire"), 
        col("GRP_SK"), 
        col("`Medical NEWBORNS_AND_OTHER_NEONATES`").cast(DoubleType()).alias("Medical NEWBORNS_AND_OTHER_NEONATES"), 
        col("`Medical CIRCULATORY_SYSTEM`").cast(DoubleType()).alias("Medical CIRCULATORY_SYSTEM"), 
        col("`Medical NERVOUS_SYSTEM`").cast(DoubleType()).alias("Medical NERVOUS_SYSTEM"), 
        col("GRP_ZIP_CD_5"), 
        col("MBR_UNIQ_KEY"), 
        col("Avg_BLUE_ACCESS"), 
        col("Avg_PCBEXTRNL"), 
        col("GRP_BUS_SUB_CAT_SH_NM"), 
        col("`Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM`")\
          .cast(DoubleType())\
          .alias("Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM"), 
        col("`Product Counter`").alias("Product Counter")
    )
