from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2168_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")), "inner")\
        .select(col("in0.MBR_GNDR_CD").alias("MBR_GNDR_CD"), col("in0.TOT_MONTHS").alias("TOT_MONTHS"), col("in0.MEMBER_AGE").alias("MEMBER_AGE"), col("in0.MBR_SK").alias("MBR_SK"), col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.MEMBER_AGE2").alias("MEMBER_AGE2"))
