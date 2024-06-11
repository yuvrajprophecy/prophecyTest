from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def P1_COBALTHS_member_l(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("MBR_RELSHP_NM2", StringType(), True), StructField("YEARMONTH", StringType(), True), StructField("MBR_INDV_BE_KEY", StringType(), True), StructField("SUB_ID", StringType(), True), StructField("MBR_RELSHP_NM", StringType(), True), StructField("MED_HOME_GRP_DESC", StringType(), True), StructField("MBR_SK", StringType(), True), StructField("YMD", StringType(), True), StructField("QHP_METAL_LVL_NM", StringType(), True), StructField("MBR_FIRST_NM", StringType(), True), StructField("SUB_CNTGS_CNTY_CD", StringType(), True), StructField("MBR_HOME_ADDR_LN_2", StringType(), True), StructField("MBR_HOME_ADDR_ZIP_CD_5", StringType(), True), StructField("MBR_BRTH_DT_SK", StringType(), True), StructField("PCP_FLAG", StringType(), True), StructField("GRP_DP_IN", StringType(), True), StructField("SUB_MKTNG_METRO_RURAL_CD", StringType(), True), StructField("GRP_NM", StringType(), True), StructField("GRP_ID", StringType(), True), StructField("MBR_DSBLTY_IN", StringType(), True), StructField("PROD_SK", StringType(), True), StructField("PROD_CAT", StringType(), True), StructField("SPIRA_BNF_ID", StringType(), True), StructField("PROV_NM", StringType(), True), StructField("MBR_GNDR_CD", StringType(), True), StructField("MBR_ENR_COBRA_IN", StringType(), True), StructField("MBR_HOME_ADDR_LN_1", StringType(), True), StructField("FEP_FLAG", StringType(), True), StructField("MBR_LAST_NM", StringType(), True), StructField("Product", StringType(), True), StructField("PRNT_GRP_SIC_NACIS_CD", StringType(), True), StructField("MBR_HOME_ADDR_ST_CD", StringType(), True), StructField("GRP_SK", StringType(), True), StructField("MED_HOME_ID", StringType(), True), StructField("MBR_UNIQ_KEY", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.member_live_cache.csv")
