from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def P1_COBALTHS_EthnicTe(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("LOB", StringType(), True), StructField("ETHNICITY", StringType(), True), StructField("GRP", StringType(), True), StructField("MBR_BEKEY", StringType(), True), StructField("LANG", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.Ethnic Tec - Oct 2023.csv")
