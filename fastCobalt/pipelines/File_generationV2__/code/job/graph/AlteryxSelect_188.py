from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_188(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Anticipated Hours`").alias("Anticipated Hours"), 
        col("`Anticipated Calls`").alias("Anticipated Calls"), 
        col("`Anticipated Lower`").alias("Anticipated Lower"), 
        col("`RT Handle Time`").alias("RT Handle Time"), 
        col("ds"), 
        col("`Anticipated Hours Lower`").alias("Anticipated Hours Lower"), 
        col("`Anticipated Upper`").alias("Anticipated Upper"), 
        col("`Anticipated Hours Upper`").alias("Anticipated Hours Upper")
    )
