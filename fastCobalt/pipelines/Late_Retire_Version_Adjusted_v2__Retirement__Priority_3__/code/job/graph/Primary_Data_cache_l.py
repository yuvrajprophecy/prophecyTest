from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Primary_Data_cache_l(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("YEARMONTH", StringType(), True), StructField("MBR_INDV_BE_KEY", StringType(), True), StructField("MBR_RELSHP_NM", StringType(), True), StructField("GRP_MKT_SIZE_CAT_NM2", StringType(), True), StructField("MBR_SK", StringType(), True), StructField("YMD", StringType(), True), StructField("SUB_MBR_SK", StringType(), True), StructField("MBR_FIRST_NM", StringType(), True), StructField("MBR_HOME_ADDR_LN_2", StringType(), True), StructField("MBR_HOME_ADDR_ZIP_CD_5", StringType(), True), StructField("PCP_FLAG", StringType(), True), StructField("GRP_NM", StringType(), True), StructField("GRP_ID", StringType(), True), StructField("GRP_BILL_LVL_NM", StringType(), True), StructField("HOST_MBR_IN", StringType(), True), StructField("AGE", StringType(), True), StructField("MBR_DSBLTY_IN", StringType(), True), StructField("EXPRNC_CAT_CD", StringType(), True), StructField("FUND_CAT_CD", StringType(), True), StructField("CLS_PLN_DESC", StringType(), True), StructField("PROD_SH_NM_DLVRY_METH_CD", StringType(), True), StructField("PROD_SH_NM", StringType(), True), StructField("GRP_TOT_EMPL_CT", StringType(), True), StructField("SPIRA_BNF_ID", StringType(), True), StructField("MBR_GNDR_CD", StringType(), True), StructField("MBR_ENR_COBRA_IN", StringType(), True), StructField("MBR_HOME_ADDR_LN_1", StringType(), True), StructField("GRP_MKT_SIZE_CAT_NM", StringType(), True), StructField("MBR_LAST_NM", StringType(), True), StructField("Product", StringType(), True), StructField("PRNT_GRP_SIC_NACIS_CD", StringType(), True), StructField("MBR_HOME_ADDR_ST_CD", StringType(), True), StructField("FNCL_LOB_CD", StringType(), True), StructField("FNCL_MKT_SEG_NM", StringType(), True), StructField("GRP_SK", StringType(), True), StructField("GRP_ZIP_CD_5", StringType(), True), StructField("MBR_UNIQ_KEY", StringType(), True), StructField("GRP_BUS_SUB_CAT_SH_NM", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("O:\\library\\Health Innovations\\PHI11\\Dedicated\\Jon Sands\\MA\\MA Targeting\\Retirement\\Iter 7.2\\Dump\\Age In V2\\Primary_Data_cache_late_retire.csv")
