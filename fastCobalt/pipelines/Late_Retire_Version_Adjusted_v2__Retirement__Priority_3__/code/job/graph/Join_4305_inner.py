from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4305_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.AGE") == col("in1.AGE")), "inner")\
        .select(col("in0.AGE").alias("AGE"), col("in0.CountDistinct_MBR_INDV_BE_KEY").alias("Positives"), col("in1.Count").alias("Negatives"))
