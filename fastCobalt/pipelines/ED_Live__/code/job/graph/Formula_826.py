from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_826(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Total Claim Spend",
          (round((col("`Total Claim Spend`").cast(DoubleType()) * lit(0.1))).cast(DoubleType()) / lit(0.1))
        )\
        .withColumn("Max Claim Line Spend", (round((col("`Max Claim Line Spend`").cast(DoubleType()) * lit(0.1))).cast(DoubleType()) / lit(0.1)))
