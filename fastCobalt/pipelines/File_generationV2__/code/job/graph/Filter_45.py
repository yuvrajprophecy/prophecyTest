from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_45(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        ((col("ds").cast(StringType()) > lit("2024-03-31")) & (col("ds").cast(StringType()) < lit("2024-07-01")))
    )
