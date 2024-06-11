from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2869_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.GRP_ID") == col("in1.GRP_ID")) & (col("in0.ACTVTY_YR_MO_SK") == col("in1.ACTVTY_YR_MO_SK"))),
          "inner"
        )\
        .select(col("in0.ACTVTY_YR_MO_SK").alias("ACTVTY_YR_MO_SK"), col("in1.`Sum_Indicator Female`").alias("Sum_Indicator Female"), col("in1.GRP_ID").alias("Right_GRP_ID"), col("in0.`Sum_Indicator Male`").alias("Sum_Indicator Male"), col("in1.ACTVTY_YR_MO_SK").alias("Right_ACTVTY_YR_MO_SK"), col("in0.GRP_ID").alias("GRP_ID"))
