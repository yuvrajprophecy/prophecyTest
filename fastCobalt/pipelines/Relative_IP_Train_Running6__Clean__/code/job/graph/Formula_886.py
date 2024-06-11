from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_886(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "Relative_IP_Month",
        when((col("Relative_IP_Month").cast(IntegerType()) == lit(- 13)), lit(- 15)).otherwise(col("Relative_IP_Month"))
    )
