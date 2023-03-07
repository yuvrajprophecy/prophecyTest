from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from basepipeline1.config.ConfigStore import *
from basepipeline1.udfs.UDFs import *

def baseDS1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("customer_id", StringType(), True), StructField("first_name", StringType(), True), StructField("last_name", StringType(), True), StructField("phone", StringType(), True), StructField("email", StringType(), True), StructField("country_code", StringType(), True), StructField("account_open_date", StringType(), True), StructField("account_flags", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/123gkc@gmail.com/CustomersDatasetInput.csv")
