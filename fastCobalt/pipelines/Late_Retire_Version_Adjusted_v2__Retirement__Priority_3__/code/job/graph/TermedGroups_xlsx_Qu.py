from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TermedGroups_xlsx_Qu(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Group Name", StringType(), True), StructField("Termed Med Members", DoubleType(), True), StructField("Group ID", StringType(), True), StructField("Termination Date", StringType(), True), StructField("Termed Med Subs", DoubleType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("O:\\library\\Alteryx\\Advanced Analytics\\Reference Workflows\\Termed Groups.xlsx|||`Sheet1$`")
