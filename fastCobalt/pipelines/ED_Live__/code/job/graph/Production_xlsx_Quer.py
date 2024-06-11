from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Production_xlsx_Quer(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ED Prediction Value", DoubleType(), True), StructField("LATEST_ED_DX", StringType(), True), StructField("ER_COUNT", DoubleType(), True), StructField("ER_COUNT_PAST_3_MONTHS", DoubleType(), True), StructField("Member Individual Business Entity Key", StringType(), True), StructField("NONEMERGENT_COUNT_PAST_60", DoubleType(), True), StructField("ED_DSCHG_DT", StringType(), True), StructField("F1", DoubleType(), True), StructField("NONEMERGENT_RATE", DoubleType(), True), StructField("ED Prediction Score", StringType(), True), StructField("Rating Date", StringType(), True), StructField("ER_COUNT_PAST_2_MONTHS", DoubleType(), True), StructField("FREQUENT_FLYER", DoubleType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\Production.xlsx|||`Sheet1$`")
