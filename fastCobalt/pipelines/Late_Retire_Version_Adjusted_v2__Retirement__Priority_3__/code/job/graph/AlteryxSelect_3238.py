from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_3238(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SUB_UNIQ_KEY"), 
        col("MBR_UNIQ_KEY"), 
        col("FIRST_DT_OF_MO"), 
        col("MBR_SK"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_RELSHP_NM"), 
        col("MBR_BRTH_DT_SK")
    )
