from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SpouseSubscriberAge(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("Spouse Age", datediff(col("FIRST_DT_OF_MO"), col("MBR_BRTH_DT_SK")))
