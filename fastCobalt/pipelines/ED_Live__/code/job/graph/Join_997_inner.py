from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_997_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`ED Prediction Score`") == col("in1.`ED Prediction Score`")), "inner")\
        .select(col("in0.`Sum_CountDistinct_Member Individual Business Entity Key`")\
        .alias("Sum_CountDistinct_Member Individual Business Entity Key"), col("in1.`ED Prediction Score`").alias("ED Prediction Score"), col("in1.`NONEMERGENT_COUNT_PAST_60 (Proposed)`").alias("NONEMERGENT_COUNT_PAST_60 (Proposed)"), col("in1.`CountDistinct_Member Individual Business Entity Key`")\
        .alias("CountDistinct_Member Individual Business Entity Key"))
