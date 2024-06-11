from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def checkpoint1_test_liv_758(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .mode("append")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\checkpoint1_test_live.csv")
