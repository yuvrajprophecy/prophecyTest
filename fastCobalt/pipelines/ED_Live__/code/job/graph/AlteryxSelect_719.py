from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_719(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SUB_ID"), 
        col("`Household ED Visit Tot Alw Amt`").alias("Household ED Visit Tot Alw Amt"), 
        col("`Household ED Visit Alw Amt`").alias("Household ED Visit Alw Amt"), 
        col("Week"), 
        col("CountDistinct_CLM_SVC_STRT_DT_SK").alias("Plan Total ED Visits")
    )
