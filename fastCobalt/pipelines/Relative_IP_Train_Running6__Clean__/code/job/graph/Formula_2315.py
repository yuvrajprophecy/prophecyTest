from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_2315(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("IP", when(col("IP").isNull(), lit(0)).otherwise(col("IP")))
