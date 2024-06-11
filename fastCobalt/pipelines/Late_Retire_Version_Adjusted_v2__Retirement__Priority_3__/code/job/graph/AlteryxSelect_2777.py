from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2777(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Member BE Key`").alias("Member BE Key"), 
        col("ACTVTY_YR_MO_SK"), 
        col("`CountDistinct_Household Member BE Key`").alias("Retiree in Household Indicator")
    )
