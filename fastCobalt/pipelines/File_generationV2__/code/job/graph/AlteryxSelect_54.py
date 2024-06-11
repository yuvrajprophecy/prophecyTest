from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_54(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Sum_Anticipated Lower`").alias("Sum_Anticipated Lower"), 
        col("`Sum_Anticipated Upper`").alias("Sum_Anticipated Upper"), 
        col("`Sum_Anticipated Calls`").alias("Sum_Anticipated Calls"), 
        col("ds").alias("Date"), 
        col("`Sum_Anticipated Hours`").alias("Anticipated Hours"), 
        col("`Sum_Anticipated Hours Upper`").alias("Upper Bound (95% CI)"), 
        col("`Sum_Anticipated Hours Lower`").alias("Lower Bound (95% CI)")
    )
