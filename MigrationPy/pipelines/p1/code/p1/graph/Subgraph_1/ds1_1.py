from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from .config import *
from p1.udfs.UDFs import *

def ds1_1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("id", StringType(), True), StructField("first_name", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/rohitjain+27@simpledatalabs.com/CustomersDatasetInput.csv")
