from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4162_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")), "inner")\
        .select(col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in1.YMD").alias("YMD"), col("in1.`Switch to Retirement`").alias("Switch to Retirement"), col("in1.Retire").alias("Retire"))
