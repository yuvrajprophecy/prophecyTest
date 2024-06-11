from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def checkpoint1_test_liv(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Sum_CLM_LN_TOT_ALW_AMT", StringType(), True), StructField("ED Visit Alw Amt", StringType(), True), StructField("Sum_CLM_LN_ALW_AMT", StringType(), True), StructField("YEARMONTH", StringType(), True), StructField("POLYPHARMACY_IN", StringType(), True), StructField("MBR_INDV_BE_KEY", StringType(), True), StructField("SUB_ID", StringType(), True), StructField("MBR_RELSHP_NM", StringType(), True), StructField("Household ED Visit Tot Alw Amt", StringType(), True), StructField("Target", StringType(), True), StructField("MED_HOME_GRP_DESC", StringType(), True), StructField("ED Visits from Members in Household (Not Member)", StringType(), True), StructField("YMD", StringType(), True), StructField("PR", StringType(), True), StructField("MED_HOME_DESC", StringType(), True), StructField("SUB_CNTGS_CNTY_CD", StringType(), True), StructField("MBR_HOME_ADDR_ZIP_CD_5", StringType(), True), StructField("DistaneMiles", StringType(), True), StructField("PCP_FLAG", StringType(), True), StructField("GRP_DP_IN", StringType(), True), StructField("SUB_MKTNG_METRO_RURAL_CD", StringType(), True), StructField("New Drug Indicator", StringType(), True), StructField("injury", StringType(), True), StructField("GRP_NM", StringType(), True), StructField("GRP_ID", StringType(), True), StructField("DRUG_COUNT", StringType(), True), StructField("Non Emergent Binary", StringType(), True), StructField("OP", StringType(), True), StructField("psych", StringType(), True), StructField("CCI_Score", StringType(), True), StructField("MBR_DSBLTY_IN", StringType(), True), StructField("TotalRVU", StringType(), True), StructField("Members in Household", StringType(), True), StructField("V", StringType(), True), StructField("DRUG_CLASS_COUNT", StringType(), True), StructField("Household ED Visit Alw Amt", StringType(), True), StructField("PROD_CAT", StringType(), True), StructField("Plan Total ED Visits", StringType(), True), StructField("ED Visits", StringType(), True), StructField("IP", StringType(), True), StructField("Mental Health Claim", StringType(), True), StructField("Week", StringType(), True), StructField("Sum_FCLTY_CLM_LOS_DAYS", StringType(), True), StructField("SPIRA_BNF_ID", StringType(), True), StructField("PROV_NM", StringType(), True), StructField("MBR_GNDR_CD", StringType(), True), StructField("MBR_ENR_COBRA_IN", StringType(), True), StructField("mbr_age", StringType(), True), StructField("ED Visited", StringType(), True), StructField("Non Emergent Likelihood", StringType(), True), StructField("FEP_FLAG", StringType(), True), StructField("RunTot_Inflection", StringType(), True), StructField("PRNT_GRP_SIC_NACIS_CD", StringType(), True), StructField("alcohol", StringType(), True), StructField("CountDistinct_CLM_ID", StringType(), True), StructField("drug", StringType(), True), StructField("Race", StringType(), True), StructField("MED_HOME_ID", StringType(), True), StructField("ED Visit Tot Alw Amt", StringType(), True), StructField("MBR_UNIQ_KEY", StringType(), True), StructField("Diagnoses", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\checkpoint1_test_live.csv")
