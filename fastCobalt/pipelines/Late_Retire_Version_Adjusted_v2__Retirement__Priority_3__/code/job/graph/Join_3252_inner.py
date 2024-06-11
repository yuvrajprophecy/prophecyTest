from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_3252_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.SUB_UNIQ_KEY") == col("in1.SUB_UNIQ_KEY"))
            & (col("in0.FIRST_DT_OF_MO") == col("in1.FIRST_DT_OF_MO"))
          ),
          "inner"
        )\
        .select(col("in0.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"), col("in1.`Spouse Age`").alias("Spouse Age"), col("in0.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"), col("in0.FIRST_DT_OF_MO").alias("FIRST_DT_OF_MO"), col("in0.Members_on_Policy").alias("Members_on_Policy"), col("in0.MBR_SK").alias("MBR_SK"), col("in0.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"), col("in0.MBR_INDV_BE_KEY").alias("Member"))
