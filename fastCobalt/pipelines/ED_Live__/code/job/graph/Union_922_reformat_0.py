from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_922_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Sum_CLM_LN_TOT_ALW_AMT").cast(StringType()).alias("Sum_CLM_LN_TOT_ALW_AMT"), 
        col("`ED Visit Alw Amt`").cast(StringType()).alias("ED Visit Alw Amt"), 
        col("Sum_CLM_LN_ALW_AMT").cast(StringType()).alias("Sum_CLM_LN_ALW_AMT"), 
        col("YEARMONTH"), 
        col("POLYPHARMACY_IN").cast(StringType()).alias("POLYPHARMACY_IN"), 
        col("MBR_INDV_BE_KEY"), 
        col("SUB_ID"), 
        col("MBR_RELSHP_NM"), 
        col("`Household ED Visit Tot Alw Amt`").cast(StringType()).alias("Household ED Visit Tot Alw Amt"), 
        col("Target"), 
        col("MED_HOME_GRP_DESC"), 
        col("`ED Visits from Members in Household (Not Member)`")\
          .alias("ED Visits from Members in Household (Not Member)"), 
        col("YMD"), 
        col("PR").cast(StringType()).alias("PR"), 
        col("MED_HOME_DESC").cast(StringType()).alias("MED_HOME_DESC"), 
        col("SUB_CNTGS_CNTY_CD"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("DistaneMiles").cast(StringType()).alias("DistaneMiles"), 
        col("PCP_FLAG"), 
        col("GRP_DP_IN"), 
        col("SUB_MKTNG_METRO_RURAL_CD"), 
        col("`New Drug Indicator`").cast(StringType()).alias("New Drug Indicator"), 
        col("injury"), 
        col("GRP_NM"), 
        col("GRP_ID"), 
        col("DRUG_COUNT").cast(StringType()).alias("DRUG_COUNT"), 
        col("`Non Emergent Binary`").alias("Non Emergent Binary"), 
        col("OP").cast(StringType()).alias("OP"), 
        col("psych"), 
        col("CCI_Score"), 
        col("MBR_DSBLTY_IN"), 
        col("TotalRVU").cast(StringType()).alias("TotalRVU"), 
        col("`Members in Household`").alias("Members in Household"), 
        col("V").cast(StringType()).alias("V"), 
        col("DRUG_CLASS_COUNT").cast(StringType()).alias("DRUG_CLASS_COUNT"), 
        col("`Household ED Visit Alw Amt`").cast(StringType()).alias("Household ED Visit Alw Amt"), 
        col("PROD_CAT"), 
        col("`Plan Total ED Visits`").alias("Plan Total ED Visits"), 
        col("`ED Visits`").alias("ED Visits"), 
        col("IP").cast(StringType()).alias("IP"), 
        col("`Mental Health Claim`").cast(StringType()).alias("Mental Health Claim"), 
        col("Week"), 
        col("Sum_FCLTY_CLM_LOS_DAYS").cast(StringType()).alias("Sum_FCLTY_CLM_LOS_DAYS"), 
        col("SPIRA_BNF_ID"), 
        col("PROV_NM"), 
        col("MBR_GNDR_CD"), 
        col("MBR_ENR_COBRA_IN"), 
        col("mbr_age"), 
        col("`ED Visited`").alias("ED Visited"), 
        col("`Non Emergent Likelihood`").alias("Non Emergent Likelihood"), 
        col("FEP_FLAG"), 
        col("RunTot_Inflection").cast(StringType()).alias("RunTot_Inflection"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("alcohol"), 
        col("CountDistinct_CLM_ID").cast(StringType()).alias("CountDistinct_CLM_ID"), 
        col("drug"), 
        col("Race"), 
        col("MED_HOME_ID"), 
        col("`ED Visit Tot Alw Amt`").cast(StringType()).alias("ED Visit Tot Alw Amt"), 
        col("MBR_UNIQ_KEY"), 
        col("Diagnoses")
    )