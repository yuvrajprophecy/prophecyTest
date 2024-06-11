from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_58(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Answer Time", (col("yhat").cast(IntegerType()) / lit(60)))\
        .withColumn("Answer Time Upper", (col("yhat_upper").cast(IntegerType()) / lit(60)))\
        .withColumn("Answer Time Lower", (col("yhat_lower").cast(IntegerType()) / lit(60)))
