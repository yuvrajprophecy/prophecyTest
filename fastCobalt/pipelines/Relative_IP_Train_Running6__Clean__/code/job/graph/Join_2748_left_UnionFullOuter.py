from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2748_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY"))
            & (col("in0.LAB_RSLT_SVC_DT_SK") == col("in1.LAB_RSLT_SVC_DT_SK"))
          ),
          "fullouter"
        )\
        .select(col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.GFR/EGFR").alias("GFR/EGFR"), col("in0.LAB_RSLT_SVC_DT_SK").alias("LAB_RSLT_SVC_DT_SK"), col("in1.albumin_test").alias("albumin_test"), col("in1.mmol").alias("mmol"), col("in1.HighestAlbumin").alias("HighestAlbumin"), col("in0.LowestGFR").alias("LowestGFR"))
