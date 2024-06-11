from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_1007_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`ED Prediction Value`").cast(StringType()).alias("ED Prediction Value"), 
        col("LATEST_ED_DX"), 
        col("ER_COUNT").cast(StringType()).alias("ER_COUNT"), 
        col("ER_COUNT_PAST_3_MONTHS").cast(StringType()).alias("ER_COUNT_PAST_3_MONTHS"), 
        col("`Member Individual Business Entity Key`").alias("Member Individual Business Entity Key"), 
        col("NONEMERGENT_COUNT_PAST_60").cast(StringType()).alias("NONEMERGENT_COUNT_PAST_60"), 
        col("ED_DSCHG_DT"), 
        col("NONEMERGENT_RATE").cast(StringType()).alias("NONEMERGENT_RATE"), 
        col("`ED Prediction Score`").alias("ED Prediction Score"), 
        col("`Rating Date`").alias("Rating Date"), 
        col("ER_COUNT_PAST_2_MONTHS").cast(StringType()).alias("ER_COUNT_PAST_2_MONTHS"), 
        col("FREQUENT_FLYER").cast(StringType()).alias("FREQUENT_FLYER"), 
        lit(None).cast(DoubleType()).alias("F1")
    )
