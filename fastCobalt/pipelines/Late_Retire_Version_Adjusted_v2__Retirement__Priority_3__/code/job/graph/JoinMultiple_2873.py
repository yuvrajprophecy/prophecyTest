from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JoinMultiple_2873(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.GRP_ID") == col("in1.GRP_ID")) & (col("in0.ACTVTY_YR_MO_SK") == col("in1.ACTVTY_YR_MO_SK"))),
          "inner"
        )\
        .join(
          in2.alias("in2"),
          ((col("in0.GRP_ID") == col("in2.GRP_ID")) & (col("in0.ACTVTY_YR_MO_SK") == col("in2.ACTVTY_YR_MO_SK"))),
          "inner"
        )\
        .select(col("in0.`Female Percentage`").alias("Female Percentage"), col("in1.`Average Age Males`").alias("Average Age Males"), col("in0.`Male Percentage`").alias("Male Percentage"), col("in1.GRP_ID").alias("GRP_ID"), col("in0.ACTVTY_YR_MO_SK").alias("ACTVTY_YR_MO_SK"), col("in2.`Average Age Females`").alias("Average Age Females"))
