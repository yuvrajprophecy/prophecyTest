from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2911_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY"))
            & (col("in0.YMD") == col("in1.Max_FIRST_DT_OF_MO"))
          ),
          "leftouter"
        )\
        .select(col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.`Female Percentage`").alias("Female Percentage"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.MBR_RELSHP_NM").alias("MBR_RELSHP_NM"), col("in0.`Average Age Males`").alias("Average Age Males"), col("in0.`Target Forecasted`").alias("Target Forecasted"), col("in0.Avg_PCB").alias("Avg_PCB"), col("in0.Avg_BCARE").alias("Avg_BCARE"), col("in0.MBR_SK").alias("MBR_SK"), col("in0.Avg_HPEXTRNL").alias("Avg_HPEXTRNL"), col("in0.YMD").alias("YMD"), col("in0.`Retired With Blue`").alias("Retired With Blue"), col("in0.`Male Percentage`").alias("Male Percentage"), col("in0.`Marketing Restricted`").alias("Marketing Restricted"), col("in0.Avg_BLUESELECT_").alias("Avg_BLUESELECT_"), col("in0.SUB_MBR_SK").alias("SUB_MBR_SK"), col("in0.`Switch to Retirement`").alias("Switch to Retirement"), col("in0.`Spouse Age`").alias("Spouse Age"), col("in0.MBR_FIRST_NM").alias("MBR_FIRST_NM"), col("in0.MBR_HOME_ADDR_LN_2").alias("MBR_HOME_ADDR_LN_2"), col("in0.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"), col("in1.`Recent Mover`").alias("Recent Mover"), col("in0.PCP_FLAG").alias("PCP_FLAG"), col("in0.`Spouse's Product`").alias("Spouse's Product"), col("in0.`Min_Dependent Age`").alias("Min_Dependent Age"), col("in0.GRP_NM").alias("GRP_NM"), col("in0.GRP_ID").alias("GRP_ID"), col("in0.GRP_BILL_LVL_NM").alias("GRP_BILL_LVL_NM"), col("in0.Avg_PC").alias("Avg_PC"), col("in0.HOST_MBR_IN").alias("HOST_MBR_IN"), col("in0.AGE").alias("AGE"), col("in0.MBR_DSBLTY_IN").alias("MBR_DSBLTY_IN"), col("in0.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"), col("in0.FUND_CAT_CD").alias("FUND_CAT_CD"), col("in0.CLS_PLN_DESC").alias("CLS_PLN_DESC"), col("in0.PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"), col("in0.Members_on_Policy").alias("Members_on_Policy"), col("in0.SUM_CCI").alias("SUM_CCI"), col("in0.`Retiree in Household Indicator`").alias("Retiree in Household Indicator"), col("in0.PROD_SH_NM").alias("PROD_SH_NM"), col("in0.GRP_TOT_EMPL_CT").alias("GRP_TOT_EMPL_CT"), col("in0.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"), col("in0.MBR_GNDR_CD").alias("MBR_GNDR_CD"), col("in0.MBR_ENR_COBRA_IN").alias("MBR_ENR_COBRA_IN"), col("in0.MBR_HOME_ADDR_LN_1").alias("MBR_HOME_ADDR_LN_1"), col("in0.`Average Age Females`").alias("Average Age Females"), col("in0.`Product Category`").alias("Product Category"), col("in0.Avg_BLUE_SELECT").alias("Avg_BLUE_SELECT"), col("in0.GRP_MKT_SIZE_CAT_NM").alias("GRP_MKT_SIZE_CAT_NM"), col("in0.MBR_LAST_NM").alias("MBR_LAST_NM"), col("in0.Product").alias("Product"), col("in0.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"), col("in0.MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"), col("in0.FNCL_LOB_CD").alias("FNCL_LOB_CD"), col("in0.FNCL_MKT_SEG_NM").alias("FNCL_MKT_SEG_NM"), col("in0.Retire").alias("Retire"), col("in0.GRP_SK").alias("GRP_SK"), col("in0.GRP_ZIP_CD_5").alias("GRP_ZIP_CD_5"), col("in0.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"), col("in0.Avg_BLUE_ACCESS").alias("Avg_BLUE_ACCESS"), col("in0.Avg_PCBEXTRNL").alias("Avg_PCBEXTRNL"), col("in0.GRP_BUS_SUB_CAT_SH_NM").alias("GRP_BUS_SUB_CAT_SH_NM"), col("in0.`Product Counter`").alias("Product Counter"))
