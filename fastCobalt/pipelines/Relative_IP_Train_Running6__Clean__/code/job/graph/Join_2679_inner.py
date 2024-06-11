from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2679_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.PROC") == col("in1.PROC_CD")), "inner")\
        .select(col("in1.PROC_CD").alias("PROC_CD"), col("in1.PROC_COUNT").alias("PROC_COUNT"), col("in1.YEARMONTH").alias("YEARMONTH"), col("in1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.PROC").alias("PROC"), col("in0.`Family Description Condensed`").alias("Family Description Condensed"), col("in0.`Family Description`").alias("Family Description"))
