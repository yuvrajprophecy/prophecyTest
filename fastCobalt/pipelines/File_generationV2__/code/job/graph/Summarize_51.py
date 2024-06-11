from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_51(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("ds"))

    return df1.agg(
        sum(col("`Anticipated Hours Lower`")).alias("Sum_Anticipated Hours Lower"), 
        sum(col("`Anticipated Lower`")).alias("Sum_Anticipated Lower"), 
        sum(col("`Anticipated Calls`")).alias("Sum_Anticipated Calls"), 
        sum(col("`Anticipated Upper`")).alias("Sum_Anticipated Upper"), 
        sum(col("`Anticipated Hours`")).alias("Sum_Anticipated Hours"), 
        sum(col("`Anticipated Hours Upper`")).alias("Sum_Anticipated Hours Upper")
    )
