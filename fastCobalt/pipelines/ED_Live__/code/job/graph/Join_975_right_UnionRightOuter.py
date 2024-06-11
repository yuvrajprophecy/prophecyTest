from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_975_right_UnionRightOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (col("in0.MBR_INDV_BE_KEY") == col("in1.`Member Individual Business Entity Key`")),
          "rightouter"
        )\
        .select(col("in1.`ED Prediction Value`").alias("ED Prediction Value"), col("in1.LATEST_ED_DX").alias("LATEST_ED_DX"), col("in0.`NONEMERGENT_COUNT_PAST_60 (OG)`").alias("NONEMERGENT_COUNT_PAST_60 (OG)"), col("in1.ER_COUNT").alias("ER_COUNT"), col("in1.ER_COUNT_PAST_3_MONTHS").alias("ER_COUNT_PAST_3_MONTHS"), col("in1.`Member Individual Business Entity Key`").alias("Member Individual Business Entity Key"), col("in1.ED_DSCHG_DT").alias("ED_DSCHG_DT"), col("in1.`ED Prediction Score`").alias("ED Prediction Score"), col("in1.ER_COUNT_PAST_2_MONTHS").alias("ER_COUNT_PAST_2_MONTHS"), col("in1.FREQUENT_FLYER").alias("FREQUENT_FLYER"))
