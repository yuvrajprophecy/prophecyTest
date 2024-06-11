from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_174(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.agg(
        sum(col("Friday")).alias("Sum_Friday"), 
        sum(col("Wednesday")).alias("Sum_Wednesday"), 
        sum(col("Monday")).alias("Sum_Monday"), 
        sum(col("Thursday")).alias("Sum_Thursday"), 
        sum(col("Tuesday")).alias("Sum_Tuesday")
    )
