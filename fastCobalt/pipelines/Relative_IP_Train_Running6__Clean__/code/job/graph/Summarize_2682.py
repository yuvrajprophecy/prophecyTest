from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_2682(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("MBR_INDV_BE_KEY"), 
        col("YEARMONTH"), 
        col("`Family Description Condensed`").alias("Family Description")
    )

    return df1.agg(sum(col("PROC_COUNT")).alias("Sum_PROC_COUNT"))
