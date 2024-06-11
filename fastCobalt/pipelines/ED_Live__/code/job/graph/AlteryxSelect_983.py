from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_983(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("ED_DSCHG_DT"), 
        col("ER_COUNT"), 
        col("ER_COUNT_PAST_3_MONTHS"), 
        col("`ED Prediction Score`").alias("ED Prediction Score"), 
        col("ER_COUNT_PAST_2_MONTHS"), 
        col("LATEST_ED_DX"), 
        col("`ED Prediction Value`").alias("ED Prediction Value"), 
        col("`Member Individual Business Entity Key`").alias("Member Individual Business Entity Key"), 
        col("FREQUENT_FLYER"), 
        col("`NONEMERGENT_RATE (OG)`").alias("NONEMERGENT_RATE"), 
        col("`NONEMERGENT_COUNT_PAST_60 (OG)`").alias("NONEMERGENT_COUNT_PAST_60")
    )
