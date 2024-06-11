from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_4356(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "Target Forecasted",
        when((col("`Target Forecasted`").cast(IntegerType()) == lit(1)), lit(2)).otherwise(col("`Target Forecasted`"))
    )
