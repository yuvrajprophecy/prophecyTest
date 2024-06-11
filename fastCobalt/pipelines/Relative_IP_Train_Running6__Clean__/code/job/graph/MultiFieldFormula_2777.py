from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_2777(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(col("mmol").isNull(), lit(0)).otherwise(col("mmol")).alias("mmol"), 
        when(col("HighestAlbumin").isNull(), lit(0)).otherwise(col("HighestAlbumin")).alias("HighestAlbumin"), 
        when(col("UniqueLabs").isNull(), lit(0)).otherwise(col("UniqueLabs")).alias("UniqueLabs"), 
        when(col("CountOfLabs").isNull(), lit(0)).otherwise(col("CountOfLabs")).alias("CountOfLabs"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("Concat_ORDER_TST_NM"), 
        col("LowestGFR")
    )
