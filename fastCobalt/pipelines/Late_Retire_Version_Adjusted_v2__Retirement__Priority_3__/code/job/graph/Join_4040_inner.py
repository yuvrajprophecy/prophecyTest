from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4040_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Value") == col("in1.DIAG_CD_SK")), "inner")\
        .select(col("in0.Name").alias("Name"), col("in1.DIAG_CD_SK").alias("DIAG_CD_SK"), col("in1.DIAG_CD_DESC").alias("DIAG_CD_DESC"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.`Received Date`").alias("Received Date"), col("in0.Value").alias("Value"))
