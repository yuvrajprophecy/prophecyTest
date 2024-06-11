from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ProdIDCrossWalk_xlsx(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("SUCat1Name", StringType(), True), StructField("SUCat2Name", StringType(), True), StructField("ProdID", StringType(), True), StructField("ProductName", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("Z:\\Alteryx\\Jon\\CHS Call Center\\Reference Files\\ProdID Cross Walk.xlsx|||`Sheet1$`")
