from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_2761(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("YEARMONTH"))

    return df1.agg(
        concat_ws(",", collect_list(col("Concat_ORDER_TST_NM"))).alias("Concat_ORDER_TST_NM"), 
        sum(col("UniqueLabs")).alias("UniqueLabs"), 
        sum(col("CountOfLabs")).alias("CountOfLabs"), 
        max(col("HighestAlbumin")).alias("HighestAlbumin"), 
        min(col("LowestGFR")).alias("LowestGFR")
    )
