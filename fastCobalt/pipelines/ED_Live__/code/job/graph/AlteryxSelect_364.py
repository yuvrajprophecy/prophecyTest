from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_364(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("MBR_DSBLTY_IN"), 
        col("mbr_age"), 
        col("MED_HOME_GRP_DESC"), 
        col("MBR_ENR_COBRA_IN"), 
        col("Week"), 
        col("MBR_UNIQ_KEY"), 
        col("FEP_FLAG"), 
        col("MED_HOME_ID"), 
        col("YMD"), 
        col("`Members in Household`").alias("Members in Household"), 
        col("MBR_GNDR_CD"), 
        col("PROD_CAT"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("PCP_FLAG"), 
        col("GRP_NM"), 
        col("SUB_ID"), 
        col("SPIRA_BNF_ID"), 
        col("PROV_NM"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("SUB_CNTGS_CNTY_CD"), 
        col("SUB_MKTNG_METRO_RURAL_CD"), 
        col("MBR_RELSHP_NM"), 
        col("GRP_DP_IN"), 
        col("GRP_ID"), 
        col("MBR_INDV_BE_KEY")
    )
