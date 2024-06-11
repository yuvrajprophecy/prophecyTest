from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2877_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.GRP_ID") == col("in1.GRP_ID")) & (col("in0.ACTVTY_YR_MO_SK") == col("in1.ACTVTY_YR_MO_SK"))),
          "inner"
        )\
        .select(col("in1.MBR_GNDR_CD").alias("MBR_GNDR_CD"), col("in0.ACTVTY_YR_MO_SK").alias("ACTVTY_YR_MO_SK"), col("in1.GRP_ID").alias("Right_GRP_ID"), col("in0.CountDistinct_MBR_INDV_BE_KEY").alias("CountDistinct_MBR_INDV_BE_KEY"), col("in1.GRP_NM").alias("Right_GRP_NM"), col("in1.ACTVTY_YR_MO_SK").alias("Right_ACTVTY_YR_MO_SK"), col("in0.GRP_ID").alias("GRP_ID"), col("in1.MBR_AGE_AT_ACTVTY_YR_MO").alias("MBR_AGE_AT_ACTVTY_YR_MO"), col("in1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"))
