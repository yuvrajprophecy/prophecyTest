from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_368_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`ED Visit Alw Amt`").alias("ED Visit Alw Amt"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("SUB_ID"), 
        col("MBR_RELSHP_NM"), 
        col("`Household ED Visit Tot Alw Amt`").cast(DoubleType()).alias("Household ED Visit Tot Alw Amt"), 
        col("MED_HOME_GRP_DESC"), 
        col("`ED Visits from Members in Household (Not Member)`")\
          .cast(StringType())\
          .alias("ED Visits from Members in Household (Not Member)"), 
        col("YMD"), 
        col("SUB_CNTGS_CNTY_CD"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("PCP_FLAG"), 
        col("GRP_DP_IN"), 
        col("SUB_MKTNG_METRO_RURAL_CD"), 
        col("GRP_NM"), 
        col("GRP_ID"), 
        col("MBR_DSBLTY_IN"), 
        col("`Members in Household`").alias("Members in Household"), 
        col("`Household ED Visit Alw Amt`").cast(DoubleType()).alias("Household ED Visit Alw Amt"), 
        col("PROD_CAT"), 
        col("`Plan Total ED Visits`").cast(StringType()).alias("Plan Total ED Visits"), 
        col("`ED Visits`").alias("ED Visits"), 
        col("Week"), 
        col("SPIRA_BNF_ID"), 
        col("PROV_NM"), 
        col("MBR_GNDR_CD"), 
        col("MBR_ENR_COBRA_IN"), 
        col("mbr_age"), 
        col("`ED Visited`").alias("ED Visited"), 
        col("FEP_FLAG"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("MED_HOME_ID"), 
        col("`ED Visit Tot Alw Amt`").alias("ED Visit Tot Alw Amt"), 
        col("MBR_UNIQ_KEY")
    )
