from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_897(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Sum_FCLTY_CLM_LOS_DAYS"), 
        col("Week"), 
        col("PR"), 
        col("CountDistinct_CLM_ID"), 
        col("IP"), 
        col("OP"), 
        col("MBR_INDV_BE_KEY")
    )
