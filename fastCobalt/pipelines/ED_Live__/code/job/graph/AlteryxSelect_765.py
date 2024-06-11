from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_765(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("DistaneMiles"), 
        col("DRUG_COUNT"), 
        col("`Household ED Visit Alw Amt`").alias("Household ED Visit Alw Amt"), 
        col("IP"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("Diagnoses"), 
        col("MBR_GNDR_CD"), 
        col("GRP_DP_IN"), 
        col("`Members in Household`").alias("Members in Household"), 
        col("CCI_Score"), 
        col("POLYPHARMACY_IN"), 
        col("MED_HOME_ID"), 
        col("RunTot_Inflection"), 
        col("DRUG_CLASS_COUNT"), 
        col("`Mental Health Claim`").alias("Mental Health Claim"), 
        col("`ED Visits from Members in Household (Not Member)`")\
          .alias("ED Visits from Members in Household (Not Member)"), 
        col("YMD"), 
        col("injury"), 
        col("Sum_CLM_LN_TOT_ALW_AMT"), 
        col("`New Drug Indicator`").alias("New Drug Indicator"), 
        col("Sum_CLM_LN_ALW_AMT"), 
        col("psych"), 
        col("MED_HOME_GRP_DESC").alias("Medical Home"), 
        col("V"), 
        col("`Non Emergent Binary`").alias("Non Emergent Binary"), 
        col("Week"), 
        col("Sum_FCLTY_CLM_LOS_DAYS").alias("LOS"), 
        col("`Plan Total ED Visits`").alias("Plan Total ED Visits"), 
        col("MED_HOME_DESC"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("GRP_ID"), 
        col("`Household ED Visit Tot Alw Amt`").alias("Household ED Visit Tot Alw Amt"), 
        col("PR"), 
        col("`Non Emergent Likelihood`").alias("Non Emergent Likelihood"), 
        col("Race"), 
        col("`ED Visited`").alias("ED Visited"), 
        col("mbr_age"), 
        col("SUB_MKTNG_METRO_RURAL_CD"), 
        col("TotalRVU"), 
        col("SPIRA_BNF_ID"), 
        col("PCP_FLAG"), 
        col("CountDistinct_CLM_ID").alias("Claims"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_DSBLTY_IN"), 
        col("Target"), 
        col("FEP_FLAG"), 
        col("`ED Visits`").alias("ED Visits"), 
        col("SUB_CNTGS_CNTY_CD"), 
        col("alcohol"), 
        col("GRP_NM"), 
        col("`ED Visit Alw Amt`").alias("ED Visit Alw Amt"), 
        col("drug"), 
        col("PROD_CAT"), 
        col("MBR_RELSHP_NM"), 
        col("MBR_ENR_COBRA_IN"), 
        col("OP"), 
        col("`ED Visit Tot Alw Amt`").alias("ED Visit Tot Alw Amt")
    )
