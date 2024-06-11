from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_4335(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("prop", (col("CountDistinct_MBR_INDV_BE_KEY") / col("Right_CountDistinct_MBR_INDV_BE_KEY")))
