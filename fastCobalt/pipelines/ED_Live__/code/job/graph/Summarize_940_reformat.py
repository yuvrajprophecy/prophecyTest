from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_940_reformat(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY").alias("Member Individual Business Entity Key"), 
        col("Buckets").alias("ED Prediction Score"), 
        col("FREQUENT_FLYER"), 
        col("positive_probability").alias("ED Prediction Value")
    )
