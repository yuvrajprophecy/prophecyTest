from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_132(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Monday Prct", (col("Monday") / col("Sum_Monday")))\
        .withColumn("Tuesday Prct", (col("Tuesday") / col("Sum_Tuesday")))\
        .withColumn("Wednesday Prct", (col("Wednesday") / col("Sum_Wednesday")))\
        .withColumn("Thursday Prct", (col("Thursday") / col("Sum_Thursday")))\
        .withColumn("Friday Prct", (col("Friday") / col("Sum_Friday")))
