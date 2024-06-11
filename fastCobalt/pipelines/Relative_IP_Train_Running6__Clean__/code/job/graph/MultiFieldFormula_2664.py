from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_2664(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(col("Primary_RX_Cost").isNull(), lit(0)).otherwise(col("Primary_RX_Cost")).alias("Primary_RX_Cost"), 
        when(col("Secondary_RX_Cost").isNull(), lit(0)).otherwise(col("Secondary_RX_Cost")).alias("Secondary_RX_Cost"), 
        when(col("Tertiary_RX_Cost").isNull(), lit(0)).otherwise(col("Tertiary_RX_Cost")).alias("Tertiary_RX_Cost"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_SK"), 
        col("Primary_RX"), 
        col("Tertiary_RX"), 
        col("Secondary_RX"), 
        col("Primary_Diagnosis")
    )
