from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_3851_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")), "inner")\
        .select(col("in0.GRP_SK").alias("GRP_SK"), col("in0.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"), col("in0.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"), col("in0.FNCL_MKT_SEG_NM").alias("FNCL_MKT_SEG_NM"), col("in0.MBR_GNDR_CD").alias("MBR_GNDR_CD"), col("in0.PROD_SH_NM").alias("PROD_SH_NM"), col("in0.GRP_TOT_EMPL_CT").alias("GRP_TOT_EMPL_CT"), col("in0.YMD").alias("YMD"), col("in0.MBR_SK").alias("MBR_SK"), col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.AGE").alias("AGE"), col("in0.MBR_HOME_ADDR_LN_1").alias("MBR_HOME_ADDR_LN_1"), col("in0.PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"), col("in0.SUB_MBR_SK").alias("SUB_MBR_SK"), col("in0.MBR_HOME_ADDR_LN_2").alias("MBR_HOME_ADDR_LN_2"), col("in0.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"), col("in0.GRP_BILL_LVL_NM").alias("GRP_BILL_LVL_NM"), col("in0.GRP_ID").alias("GRP_ID"), col("in0.GRP_MKT_SIZE_CAT_NM").alias("GRP_MKT_SIZE_CAT_NM"), col("in0.CLS_PLN_DESC").alias("CLS_PLN_DESC"), col("in0.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"), col("in0.MBR_LAST_NM").alias("MBR_LAST_NM"), col("in0.HOST_MBR_IN").alias("HOST_MBR_IN"), col("in0.GRP_ZIP_CD_5").alias("GRP_ZIP_CD_5"), col("in0.FUND_CAT_CD").alias("FUND_CAT_CD"), col("in0.GRP_BUS_SUB_CAT_SH_NM").alias("GRP_BUS_SUB_CAT_SH_NM"), col("in0.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"), col("in0.PCP_FLAG").alias("PCP_FLAG"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"), col("in0.MBR_DSBLTY_IN").alias("MBR_DSBLTY_IN"), col("in0.FNCL_LOB_CD").alias("FNCL_LOB_CD"), col("in1.MBR_INDV_BE_KEY").alias("Right_MBR_INDV_BE_KEY"), col("in0.MBR_FIRST_NM").alias("MBR_FIRST_NM"), col("in0.GRP_NM").alias("GRP_NM"), col("in0.MBR_RELSHP_NM").alias("MBR_RELSHP_NM"), col("in0.Product").alias("Product"), col("in0.MBR_ENR_COBRA_IN").alias("MBR_ENR_COBRA_IN"))
