from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_725(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("MBR_RELSHP_NM2"), 
        col("MBR_DSBLTY_IN"), 
        col("MED_HOME_GRP_DESC"), 
        col("Product"), 
        col("MBR_ENR_COBRA_IN"), 
        col("MBR_UNIQ_KEY"), 
        col("MED_HOME_ID"), 
        col("YMD"), 
        col("MBR_HOME_ADDR_LN_2"), 
        col("MBR_LAST_NM"), 
        col("MBR_GNDR_CD"), 
        col("MBR_SK"), 
        col("PROD_CAT"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("MBR_HOME_ADDR_LN_1"), 
        col("PCP_FLAG"), 
        col("GRP_NM"), 
        col("SUB_ID"), 
        col("SPIRA_BNF_ID"), 
        col("PROV_NM"), 
        col("MBR_HOME_ADDR_ST_CD"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("MBR_FIRST_NM"), 
        col("MBR_BRTH_DT_SK"), 
        col("PROD_SK"), 
        col("QHP_METAL_LVL_NM"), 
        col("SUB_CNTGS_CNTY_CD"), 
        col("SUB_MKTNG_METRO_RURAL_CD"), 
        col("MBR_RELSHP_NM"), 
        col("GRP_DP_IN"), 
        col("GRP_ID"), 
        col("GRP_SK"), 
        col("MBR_INDV_BE_KEY"), 
        col("FEP_FLAG")
    )
