from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2874(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Sum_Indicator Male`").alias("Sum_Indicator Male"), 
        col("`Female Percentage`").alias("Female Percentage"), 
        col("`Sum_Indicator Female`").alias("Sum_Indicator Female"), 
        col("ACTVTY_YR_MO_SK"), 
        col("`Male Percentage`").alias("Male Percentage"), 
        col("GRP_ID")
    )
