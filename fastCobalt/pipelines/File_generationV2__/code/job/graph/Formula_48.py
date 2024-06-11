from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_48(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Anticipated Calls", (col("`RT Percent of Calls`") * col("yhat")))\
        .withColumn("Anticipated Lower", (col("`RT Percent of Calls`") * col("yhat_lower")))\
        .withColumn("Anticipated Upper", (col("`RT Percent of Calls`") * col("yhat_upper")))\
        .withColumn("Anticipated Hours", (col("`Anticipated Calls`") / col("`RT Handle Time`")))\
        .withColumn("Anticipated Hours Upper", (col("`Anticipated Upper`") / col("`RT Handle Time`")))\
        .withColumn("Anticipated Hours Lower", (col("`Anticipated Lower`") / col("`RT Handle Time`")))
