from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Production_xlsx_Quer_1012(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .mode("overwrite")\
        .option("separator", "")\
        .option("header", False)\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\Production.xlsx|||Sheet1")
