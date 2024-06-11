from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_17_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.YMD") == col("in1.YMD")), "inner")\
        .select(col("in0.Sum_Sum_TTLHours").alias("Sum_Sum_TTLHours"), col("in0.YMD").alias("YMD"), col("in1.`TTL Calls`").alias("TTL Calls"), col("in0.`Call Type`").alias("Call Type"), col("in0.Sum_Sum_TTLCalls").alias("Sum_Sum_TTLCalls"))
