from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_3948(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "High Deductible Ind",
        when(col("CLS_PLN_DESC").contains(lit("HP")), lit(1)).otherwise(lit(0))
    )
