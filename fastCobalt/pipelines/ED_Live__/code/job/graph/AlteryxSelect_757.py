from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_757(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("psych"), 
        col("MBR_DSBLTY_IN"), 
        col("mbr_age"), 
        col("MED_HOME_GRP_DESC"), 
        col("Diagnoses"), 
        col("alcohol"), 
        col("DRUG_CLASS_COUNT"), 
        col("MBR_ENR_COBRA_IN"), 
        col("`ED Visited`").alias("ED Visited"), 
        col("Week"), 
        col("`Household ED Visit Alw Amt`").alias("Household ED Visit Alw Amt"), 
        col("MBR_UNIQ_KEY"), 
        col("FEP_FLAG"), 
        col("MED_HOME_ID"), 
        col("YMD"), 
        col("`Members in Household`").alias("Members in Household"), 
        col("injury"), 
        col("CCI_Score"), 
        col("MBR_GNDR_CD"), 
        col("PROD_CAT"), 
        col("Target"), 
        col("drug"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("GRP_NM"), 
        col("`ED Visits`").alias("ED Visits"), 
        col("MBR_INDV_BE_KEY"), 
        col("SUB_ID"), 
        col("`ED Visits from Members in Household (Not Member)`")\
          .alias("ED Visits from Members in Household (Not Member)"), 
        col("SPIRA_BNF_ID"), 
        col("PROV_NM"), 
        col("Race"), 
        col("DRUG_COUNT"), 
        col("`ED Visit Tot Alw Amt`").alias("ED Visit Tot Alw Amt"), 
        col("POLYPHARMACY_IN"), 
        col("`ED Visit Alw Amt`").alias("ED Visit Alw Amt"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("`Non Emergent Binary`").alias("Non Emergent Binary"), 
        col("SUB_CNTGS_CNTY_CD"), 
        col("SUB_MKTNG_METRO_RURAL_CD"), 
        col("MBR_RELSHP_NM"), 
        col("`Non Emergent Likelihood`").alias("Non Emergent Likelihood"), 
        col("GRP_DP_IN"), 
        col("`Plan Total ED Visits`").alias("Plan Total ED Visits"), 
        col("`Household ED Visit Tot Alw Amt`").alias("Household ED Visit Tot Alw Amt"), 
        col("DistaneMiles"), 
        col("GRP_ID"), 
        col("PCP_FLAG")
    )
