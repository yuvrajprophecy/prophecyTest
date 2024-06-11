from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def OARSPCTICDATA2024Q1_(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("TTLCalls", StringType(), True), StructField("UnitName", StringType(), True), StructField("ProdID", StringType(), True), StructField("TTLHours", StringType(), True), StructField("TicDate", StringType(), True), StructField("UserID", StringType(), True), StructField("NetworkName", StringType(), True), StructField("ProductName", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("Z:\\Alteryx\\Jon\\CHS Call Center\\OARS PC TICDATA\\OARS PC TICDATA 2024Q1.csv")
