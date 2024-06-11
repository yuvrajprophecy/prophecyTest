from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def EDHistory_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in1.SUB_ID") == col("in0.SUB_ID")) & (col("in1.Week") == col("in0.Week"))),
          "leftanti"
        )\
        .select(col("in0.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"), col("in0.`Members in Household`").alias("Members in Household"), col("in0.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"), col("in0.SUB_CNTGS_CNTY_CD").alias("SUB_CNTGS_CNTY_CD"), col("in0.GRP_NM").alias("GRP_NM"), col("in0.PROV_NM").alias("PROV_NM"), col("in0.MED_HOME_ID").alias("MED_HOME_ID"), col("in0.MBR_DSBLTY_IN").alias("MBR_DSBLTY_IN"), col("in0.`ED Visit Tot Alw Amt`").alias("ED Visit Tot Alw Amt"), col("in0.SUB_MKTNG_METRO_RURAL_CD").alias("SUB_MKTNG_METRO_RURAL_CD"), col("in0.PROD_CAT").alias("PROD_CAT"), col("in0.MBR_RELSHP_NM").alias("MBR_RELSHP_NM"), col("in0.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"), col("in0.YMD").alias("YMD"), col("in0.`ED Visit Alw Amt`").alias("ED Visit Alw Amt"), col("in0.Week").alias("Week"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.GRP_ID").alias("GRP_ID"), col("in0.MED_HOME_GRP_DESC").alias("MED_HOME_GRP_DESC"), col("in0.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"), col("in0.mbr_age").alias("mbr_age"), col("in0.`ED Visits`").alias("ED Visits"), col("in0.FEP_FLAG").alias("FEP_FLAG"), col("in0.`ED Visited`").alias("ED Visited"), col("in0.MBR_GNDR_CD").alias("MBR_GNDR_CD"), col("in0.MBR_ENR_COBRA_IN").alias("MBR_ENR_COBRA_IN"), col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.SUB_ID").alias("SUB_ID"), col("in0.GRP_DP_IN").alias("GRP_DP_IN"), col("in0.PCP_FLAG").alias("PCP_FLAG"))
