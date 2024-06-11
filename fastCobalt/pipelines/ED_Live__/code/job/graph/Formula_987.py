from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_987(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "NONEMERGENT_RATE (OG)",
          (round((col("`NONEMERGENT_RATE (OG)`").cast(IntegerType()) * lit(10))).cast(IntegerType()) / lit(10))
        )\
        .withColumn("NONEMERGENT_RATE (Proposed)", (round((col("`NONEMERGENT_RATE (Proposed)`").cast(IntegerType()) * lit(10))).cast(IntegerType()) / lit(10)))
