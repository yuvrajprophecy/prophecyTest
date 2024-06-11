from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_3251_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Member"), 
        col("MBR_SK"), 
        col("`Spouse Age`").alias("Spouse Age"), 
        col("MBR_BRTH_DT_SK").cast(StringType()).alias("MBR_BRTH_DT_SK"), 
        col("Members_on_Policy"), 
        col("SUB_UNIQ_KEY"), 
        col("FIRST_DT_OF_MO"), 
        col("MBR_UNIQ_KEY"), 
        lit(None).cast(StringType()).alias("Min_Dependent Age")
    )
