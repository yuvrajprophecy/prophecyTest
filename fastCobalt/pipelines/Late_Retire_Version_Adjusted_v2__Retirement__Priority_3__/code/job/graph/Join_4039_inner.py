from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4039_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Value") == col("in1.PROC_CD_SK")), "inner")\
        .select(col("in1.PROC_CD_SK").alias("PROC_CD_SK"), col("in0.Name").alias("Name"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.`Received Date`").alias("Received Date"), col("in1.PROC_CD_DESC").alias("PROC_CD_DESC"), col("in0.Value").alias("Value"))
