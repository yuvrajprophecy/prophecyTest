from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_377_right_UnionRightOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.Week") == col("in1.Week"))),
          "rightouter"
        )\
        .select(col("in1.`ED Visit Alw Amt`").alias("ED Visit Alw Amt"), col("in1.YEARMONTH").alias("YEARMONTH"), col("in1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in1.SUB_ID").alias("SUB_ID"), col("in1.MBR_RELSHP_NM").alias("MBR_RELSHP_NM"), col("in1.`Household ED Visit Tot Alw Amt`").alias("Household ED Visit Tot Alw Amt"), col("in0.Target").alias("Target"), col("in1.MED_HOME_GRP_DESC").alias("MED_HOME_GRP_DESC"), col("in1.`ED Visits from Members in Household (Not Member)`")\
        .alias("ED Visits from Members in Household (Not Member)"), col("in1.YMD").alias("YMD"), col("in1.SUB_CNTGS_CNTY_CD").alias("SUB_CNTGS_CNTY_CD"), col("in1.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"), col("in1.PCP_FLAG").alias("PCP_FLAG"), col("in1.GRP_DP_IN").alias("GRP_DP_IN"), col("in1.SUB_MKTNG_METRO_RURAL_CD").alias("SUB_MKTNG_METRO_RURAL_CD"), col("in1.injury").alias("injury"), col("in1.GRP_NM").alias("GRP_NM"), col("in1.GRP_ID").alias("GRP_ID"), col("in1.`Non Emergent Binary`").alias("Non Emergent Binary"), col("in1.psych").alias("psych"), col("in1.MBR_DSBLTY_IN").alias("MBR_DSBLTY_IN"), col("in1.`Members in Household`").alias("Members in Household"), col("in1.`Household ED Visit Alw Amt`").alias("Household ED Visit Alw Amt"), col("in1.PROD_CAT").alias("PROD_CAT"), col("in1.`Plan Total ED Visits`").alias("Plan Total ED Visits"), col("in1.`ED Visits`").alias("ED Visits"), col("in1.Week").alias("Week"), col("in1.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"), col("in1.PROV_NM").alias("PROV_NM"), col("in1.MBR_GNDR_CD").alias("MBR_GNDR_CD"), col("in1.MBR_ENR_COBRA_IN").alias("MBR_ENR_COBRA_IN"), col("in1.mbr_age").alias("mbr_age"), col("in1.`ED Visited`").alias("ED Visited"), col("in1.`Non Emergent Likelihood`").alias("Non Emergent Likelihood"), col("in1.FEP_FLAG").alias("FEP_FLAG"), col("in1.PRNT_GRP_SIC_NACIS_CD").alias("PRNT_GRP_SIC_NACIS_CD"), col("in1.alcohol").alias("alcohol"), col("in1.drug").alias("drug"), col("in1.MED_HOME_ID").alias("MED_HOME_ID"), col("in1.`ED Visit Tot Alw Amt`").alias("ED Visit Tot Alw Amt"), col("in1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"), col("in1.Diagnoses").alias("Diagnoses"))
