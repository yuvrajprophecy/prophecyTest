from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def EDHistory_left(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.Week") == col("in1.Week"))),
          "leftanti"
        )\
        .select(col("in0.CountDistinct_CLM_SVC_STRT_DT_SK").alias("CountDistinct_CLM_SVC_STRT_DT_SK"), col("in0.`ED Visit Tot Alw Amt`").alias("ED Visit Tot Alw Amt"), col("in0.`ED Visit Alw Amt`").alias("ED Visit Alw Amt"), col("in0.Week").alias("Week"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.`ED Visited`").alias("ED Visited"))
