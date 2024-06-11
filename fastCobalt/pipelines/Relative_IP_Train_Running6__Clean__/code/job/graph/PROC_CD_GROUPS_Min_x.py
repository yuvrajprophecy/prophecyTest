from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def PROC_CD_GROUPS_Min_x(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("PROC", StringType(), True), StructField("Family Description", StringType(), True), StructField("Family Description Condensed", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("O:\\library\\Health Innovations\\PHI11\\Dedicated\\Clinical Analytics\\2017 HCC Study\\Alteryx\\PROC_CD_GROUPS_Min.xlsx|||`PROC_CD_GROUPS$`")
