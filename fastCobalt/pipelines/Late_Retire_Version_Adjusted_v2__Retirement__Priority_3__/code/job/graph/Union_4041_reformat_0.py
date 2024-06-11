from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_4041_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("PROC_CD_DESC").cast(StringType()).alias("PROC_CD_DESC"), 
        col("PROC_CD_SK").cast(StringType()).alias("PROC_CD_SK"), 
        col("Name"), 
        col("`Received Date`").alias("Received Date"), 
        col("Value"), 
        lit(None).cast(StringType()).alias("DIAG_CD_DESC"), 
        lit(None).cast(StringType()).alias("DIAG_CD_SK")
    )
