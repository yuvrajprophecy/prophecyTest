from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_136(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Date"), 
        col("Time"), 
        col("Prct"), 
        col("`Anticipated Hours`").alias("Anticipated Hours"), 
        col("`Upper Bound (95% CI)`").alias("Upper Bound (95% CI)"), 
        col("`Lower Bound (95% CI)`").alias("Lower Bound (95% CI)"), 
        col("`Sum_Anticipated Calls`").alias("Calls"), 
        col("`Sum_Anticipated Lower`").alias("Calls Lower"), 
        col("`Sum_Anticipated Upper`").alias("Calls Upper")
    )
