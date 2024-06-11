from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2312_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_SK") == col("in1.MBR_SK")) & (col("in0.YEARMONTH") == col("in1.YEARMONTH"))),
          "fullouter"
        )\
        .select(col("in0.MBR_SK").alias("MBR_SK"), col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.IP").alias("IP"))
