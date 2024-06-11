from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def yxmc_4330_window(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "PERCENT_RANK_COLUMN",
        percent_rank().over(Window.partitionBy().orderBy(monotonically_increasing_id().asc()))
    )
