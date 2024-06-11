from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Dates_xlsx_Query__Sh(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("YMD", StringType(), True)]))\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("Z:\\Alteryx\\Jon\\CHS Call Center\\Reference Files\\Dates.xlsx|||`Sheet1$`")
