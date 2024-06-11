from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_4183_reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_RELSHP_NM"), 
        col("`Target Forecasted`").alias("Target Forecasted"), 
        col("Avg_PCB"), 
        col("Avg_BCARE"), 
        col("MBR_SK"), 
        col("Avg_HPEXTRNL"), 
        col("YMD"), 
        col("`Marketing Restricted`").alias("Marketing Restricted"), 
        col("Avg_BLUESELECT_"), 
        col("SUB_MBR_SK"), 
        col("`Switch to Retirement`").alias("Switch to Retirement"), 
        col("MBR_FIRST_NM"), 
        col("MBR_HOME_ADDR_LN_2"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("PCP_FLAG"), 
        col("GRP_NM"), 
        col("GRP_ID"), 
        col("GRP_BILL_LVL_NM"), 
        col("Avg_PC"), 
        col("HOST_MBR_IN"), 
        col("AGE"), 
        col("MBR_DSBLTY_IN"), 
        col("EXPRNC_CAT_CD"), 
        col("FUND_CAT_CD"), 
        col("CLS_PLN_DESC"), 
        col("PROD_SH_NM_DLVRY_METH_CD"), 
        col("PROD_SH_NM"), 
        col("GRP_TOT_EMPL_CT"), 
        col("SPIRA_BNF_ID"), 
        col("MBR_GNDR_CD"), 
        col("MBR_ENR_COBRA_IN"), 
        col("MBR_HOME_ADDR_LN_1"), 
        col("`Product Category`").alias("Product Category"), 
        col("Avg_BLUE_SELECT"), 
        col("GRP_MKT_SIZE_CAT_NM"), 
        col("MBR_LAST_NM"), 
        col("Product"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("MBR_HOME_ADDR_ST_CD"), 
        col("FNCL_LOB_CD"), 
        col("FNCL_MKT_SEG_NM"), 
        col("Retire"), 
        col("GRP_SK"), 
        col("GRP_ZIP_CD_5"), 
        col("MBR_UNIQ_KEY"), 
        col("Avg_BLUE_ACCESS"), 
        col("Avg_PCBEXTRNL"), 
        col("GRP_BUS_SUB_CAT_SH_NM"), 
        col("`Product Counter`").alias("Product Counter"), 
        lit(None).cast(StringType()).alias("Retired With Blue")
    )
