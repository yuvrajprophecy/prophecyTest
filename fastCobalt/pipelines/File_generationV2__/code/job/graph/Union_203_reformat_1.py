from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_203_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Time Distributed Hours Upper`").cast(StringType()).alias("Time Distributed Hours Upper"), 
        col("`Calls Lower`").cast(DoubleType()).alias("Calls Lower"), 
        col("`Time Distributed Calls Upper`").cast(StringType()).alias("Time Distributed Calls Upper"), 
        col("`Upper Bound (95% CI)`").cast(DoubleType()).alias("Upper Bound (95% CI)"), 
        col("`Time Distributed Hours Lower`").cast(StringType()).alias("Time Distributed Hours Lower"), 
        col("Prct").cast(StringType()).alias("Prct"), 
        col("`Time Distributed Calls Lower`").cast(StringType()).alias("Time Distributed Calls Lower"), 
        col("`Time Distributed Hours`").cast(StringType()).alias("Time Distributed Hours"), 
        col("`Anticipated Hours`").cast(DoubleType()).alias("Anticipated Hours"), 
        col("`Calls Upper`").cast(DoubleType()).alias("Calls Upper"), 
        col("`Time Distributed Calls`").cast(StringType()).alias("Time Distributed Calls"), 
        col("Date"), 
        col("Calls").cast(DoubleType()).alias("Calls"), 
        col("`Lower Bound (95% CI)`").cast(DoubleType()).alias("Lower Bound (95% CI)"), 
        col("Time")
    )
