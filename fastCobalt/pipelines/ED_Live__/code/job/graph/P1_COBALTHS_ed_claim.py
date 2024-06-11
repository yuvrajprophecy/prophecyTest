from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def P1_COBALTHS_ed_claim(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("MBR_INDV_BE_KEY", StringType(), True), StructField("CLM_ID", StringType(), True), StructField("EXP_SUB_CAT_CD", StringType(), True), StructField("CLM_LN_TOT_ALW_AMT", StringType(), True), StructField("DIAG_CD", StringType(), True), StructField("CLM_SVC_STRT_DT_SK", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.ed_claim_diag.csv")
