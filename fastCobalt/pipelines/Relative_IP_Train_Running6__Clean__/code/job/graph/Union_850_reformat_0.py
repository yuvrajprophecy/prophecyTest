from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_850_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        lit(None).cast(StringType()).alias("YEARMONTH"), 
        lit(None).cast(StringType()).alias("Relative_IP_Month")
    )
