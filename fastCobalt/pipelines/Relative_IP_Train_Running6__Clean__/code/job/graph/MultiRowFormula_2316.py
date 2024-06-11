from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2316(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("IP_PreviousMonth", col("IP_lag1")).drop("IP_lag1")
