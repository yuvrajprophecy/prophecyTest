from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2658(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("YEARMONTH"), 
        col("Primary_Diagnosis").alias("OfLast6_Main_Diag"), 
        col("Sum_Primary_Diagnosis_Cost").alias("OfLast6_Main_Diag_Cost")
    )
