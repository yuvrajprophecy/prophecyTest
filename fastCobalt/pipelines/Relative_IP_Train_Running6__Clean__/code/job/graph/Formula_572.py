from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_572(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("IP Month Reference", col("Relative_IP_Month"))\
        .withColumn("Relative_IP_Month", when(col("Relative_IP_Month").isNull(), lit(- 13))\
        .when((col("Relative_IP_Month").cast(IntegerType()) < lit(- 13)), lit(- 13))\
        .otherwise(col("Relative_IP_Month")))
