from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_4184(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (((col("`Target Forecasted`").cast(IntegerType()) != lit(2)) & (col("Retire").cast(IntegerType()) == lit(0))) & (col("AGE").cast(IntegerType()) >= lit(66)))
          & (col("AGE").cast(IntegerType()) <= lit(70))
        )
    )
