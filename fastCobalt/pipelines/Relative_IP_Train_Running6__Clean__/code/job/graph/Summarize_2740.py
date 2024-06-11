from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_2740(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("YEARMONTH"))

    return df1.agg(
        max(col("HighestAlbumin")).alias("HighestAlbumin"), 
        max(col("GFR/EGFR")).alias("GFR/EGFR"), 
        min(col("LowestGFR")).alias("LowestGFR"), 
        max(col("albumin_test")).alias("albumin_test"), 
        max(col("mmol")).alias("mmol")
    )
