from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2775_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.YEARMONTH") == col("in1.YEARMONTH"))),
          "fullouter"
        )\
        .select(col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.GFR/EGFR").alias("GFR/EGFR"), col("in1.Concat_ORDER_TST_NM").alias("Concat_ORDER_TST_NM"), col("in0.albumin_test").alias("albumin_test"), col("in1.UniqueLabs").alias("UniqueLabs"), col("in0.mmol").alias("mmol"), col("in0.HighestAlbumin").alias("HighestAlbumin"), col("in0.LowestGFR").alias("LowestGFR"), col("in1.CountOfLabs").alias("CountOfLabs"))
