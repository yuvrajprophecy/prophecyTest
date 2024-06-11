from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4175_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in1.Max_Max_YEARMONTH") == col("in0.Max_YEARMONTH")), "leftanti")\
        .select(col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.Max_YEARMONTH").alias("Max_YEARMONTH"))
