from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2646_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.YEARMONTH") == col("in1.YEARMONTH"))),
          "fullouter"
        )\
        .select(col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.MBR_SK").alias("MBR_SK"), col("in0.Primary_RX").alias("Primary_RX"), col("in0.Tertiary_RX").alias("Tertiary_RX"), col("in0.Secondary_RX").alias("Secondary_RX"), col("in0.Secondary_RX_Cost").alias("Secondary_RX_Cost"), col("in0.Tertiary_RX_Cost").alias("Tertiary_RX_Cost"), col("in0.Primary_Diagnosis").alias("Primary_Diagnosis"), col("in0.Primary_RX_Cost").alias("Primary_RX_Cost"))
