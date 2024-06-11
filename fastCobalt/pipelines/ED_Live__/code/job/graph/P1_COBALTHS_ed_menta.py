from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def P1_COBALTHS_ed_menta(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("MBR_INDV_BE_KEY", StringType(), True), StructField("DIAG_CD_1_SK", StringType(), True), StructField("DIAG_CD_2_SK", StringType(), True), StructField("DIAG_CD_3_SK", StringType(), True), StructField("DIAG_CD", StringType(), True), StructField("CLM_SVC_STRT_DT_SK", StringType(), True), StructField("MBR_UNIQ_KEY", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.ed_mental_health.csv")
