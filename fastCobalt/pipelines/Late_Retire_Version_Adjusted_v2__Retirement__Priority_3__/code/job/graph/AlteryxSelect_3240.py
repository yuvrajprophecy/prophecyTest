from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_3240(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("SUB_UNIQ_KEY"), col("FIRST_DT_OF_MO"), col("Count").alias("Members_on_Policy"))
