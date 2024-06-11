from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_717(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("CountDistinct_CLM_SVC_STRT_DT_SK"), 
        col("`ED Visited`").alias("ED Visited"), 
        col("Week"), 
        col("MBR_INDV_BE_KEY"), 
        col("`ED Visit Tot Alw Amt`").alias("ED Visit Tot Alw Amt"), 
        col("`ED Visit Alw Amt`").alias("ED Visit Alw Amt")
    )
