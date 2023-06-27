from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from p1.config.ConfigStore import *
from p1.udfs.UDFs import *

def ds1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("id", StringType(), True), StructField("first_name", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/rohitjain+27@simpledatalabs.com/CustomersDatasetInput.csv")
