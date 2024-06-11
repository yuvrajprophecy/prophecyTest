from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_137(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Time Distributed Calls", (col("Calls") * col("Prct")))\
        .withColumn("Time Distributed Calls Lower", (col("`Calls Lower`") * col("Prct")))\
        .withColumn("Time Distributed Calls Upper", (col("`Calls Upper`") * col("Prct")))\
        .withColumn("Time Distributed Hours", (col("`Anticipated Hours`") * col("Prct")))\
        .withColumn("Time Distributed Hours Lower", (col("`Lower Bound (95% CI)`") * col("Prct")))\
        .withColumn("Time Distributed Hours Upper", (col("`Upper Bound (95% CI)`") * col("Prct")))
