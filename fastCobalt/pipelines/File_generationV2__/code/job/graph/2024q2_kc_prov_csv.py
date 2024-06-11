from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def 2024q2_kc_prov_csv(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Field_1", StringType(), True), StructField("yhat_upper", StringType(), True), StructField("yhat", StringType(), True), StructField("ds", StringType(), True), StructField("yhat_lower", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("Z:\\Alteryx\\Jon\\CHS Call Center\\Predictions\\2024q2_kc_prov.csv")
