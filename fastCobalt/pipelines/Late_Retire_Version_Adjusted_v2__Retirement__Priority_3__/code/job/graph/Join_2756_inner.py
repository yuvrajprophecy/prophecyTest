from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2756_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.SUB_SK") == col("in1.SUB_SK")), "inner")\
        .select(col("in1.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"), col("in1.PROD_SH_NM").alias("PROD_SH_NM"), col("in1.FIRST_DT_OF_MO").alias("FIRST_DT_OF_MO"), col("in1.SUB_SK").alias("SUB_SK"), col("in1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"))
