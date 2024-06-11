from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2882(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Female Percentage`").alias("Female Percentage"), 
        col("`Average Age Females`").alias("Average Age Females"), 
        col("`Average Age Males`").alias("Average Age Males"), 
        col("`Male Percentage`").alias("Male Percentage"), 
        col("GRP_ID"), 
        col("ACTVTY_YR_MO_SK")
    )
