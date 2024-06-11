from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def P1_COBALTHS_ed_rx_su_1051(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("POLYPHARMACY_IN", StringType(), True), StructField("MBR_INDV_BE_KEY", StringType(), True), StructField("DRUG_COUNT", StringType(), True), StructField("DRUG_CLASS_COUNT", StringType(), True), StructField("YR_MO", StringType(), True), StructField("MBR_UNIQ_KEY", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.ed_rx_subset_23-24_pt1.csv")
