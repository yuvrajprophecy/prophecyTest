from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2908_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in1.MBR_INDV_BE_KEY") == col("in0.MBR_INDV_BE_KEY")), "leftanti")\
        .select(col("in0.`mover indicator`").alias("mover indicator"), col("in0.MBR_SK").alias("MBR_SK"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.RESIDENTS").alias("RESIDENTS"), col("in0.`New Field`").alias("New Field"), col("in0.ADDR_COMPLETE_APT_FIXED").alias("ADDR_COMPLETE_APT_FIXED"), col("in0.FIRST_DT_OF_MO").alias("FIRST_DT_OF_MO"))
