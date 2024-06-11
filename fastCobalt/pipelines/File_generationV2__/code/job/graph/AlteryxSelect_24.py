from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_24(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YMD"), 
        col("UnitName"), 
        col("NetworkName"), 
        col("ProdID"), 
        col("ProductName"), 
        col("TTLCalls"), 
        col("TTLHours"), 
        col("UserID"), 
        col("`Day of Week`").alias("Day of Week")
    )
