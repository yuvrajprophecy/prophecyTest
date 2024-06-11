from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_160(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Date"), 
        col("`Anticipated Hours`").alias("Anticipated Hours"), 
        col("`Upper Bound (95% CI)`").alias("Upper Bound (95% CI)"), 
        col("`Lower Bound (95% CI)`").alias("Lower Bound (95% CI)"), 
        col("Time"), 
        col("Prct"), 
        col("Calls"), 
        col("`Calls Lower`").alias("Calls Lower"), 
        col("`Calls Upper`").alias("Calls Upper"), 
        col("`Time Distributed Calls`").alias("Time Distributed Calls"), 
        col("`Time Distributed Calls Lower`").alias("Time Distributed Calls Lower"), 
        col("`Time Distributed Calls Upper`").alias("Time Distributed Calls Upper"), 
        col("`Time Distributed Hours`").alias("Time Distributed Hours"), 
        col("`Time Distributed Hours Lower`").alias("Time Distributed Hours Lower"), 
        col("`Time Distributed Hours Upper`").alias("Time Distributed Hours Upper")
    )
