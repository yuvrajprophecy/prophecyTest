from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_889_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.Week") == col("in1.Week"))),
          "leftouter"
        )\
        .select(col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in1.PR").alias("PR"), col("in1.OP").alias("OP"), col("in1.IP").alias("IP"), col("in0.Week").alias("Week"), col("in1.Sum_FCLTY_CLM_LOS_DAYS").alias("Sum_FCLTY_CLM_LOS_DAYS"), col("in1.Week").alias("Right_Week"), col("in1.CountDistinct_CLM_ID").alias("CountDistinct_CLM_ID"))
