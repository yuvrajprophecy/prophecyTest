from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_4307(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Negative Rate", (lit(1) - col("`Positive Rate`").cast(IntegerType())))\
        .withColumn("Total Selection", (col("Negatives") / col("`Negative Rate`")))\
        .withColumn("Positive Selection", (col("`Positive Rate`") * col("`Total Selection`")))\
        .withColumn("Negative selection", col("Negatives"))
