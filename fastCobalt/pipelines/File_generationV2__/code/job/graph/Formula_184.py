from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_184(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("yhat", when((col("yhat").cast(IntegerType()) < lit(0)), lit(0)).otherwise(col("yhat")))\
        .withColumn("yhat_lower", when((col("yhat_lower").cast(IntegerType()) < lit(0)), lit(0)).otherwise(col("yhat_lower")))\
        .withColumn("yhat_upper", when((col("yhat_upper").cast(IntegerType()) < lit(0)), lit(0)).otherwise(col("yhat_upper")))
