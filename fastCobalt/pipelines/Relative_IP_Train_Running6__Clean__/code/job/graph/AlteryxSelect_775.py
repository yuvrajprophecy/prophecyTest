from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_775(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("Tertiary_Diagnosis_Running3"), 
        col("OfLast6_Main_Diag"), 
        col("OfLast6_Main_Diag_Cost"), 
        col("MBR_INDV_BE_KEY"), 
        col("Tertiary_DiagnosisCosts_Running3"), 
        col("Primary_Diagnosis_Running3"), 
        col("Primary_DiagnosisCosts_Running3"), 
        col("Secondary_Diagnosis_Running3"), 
        col("Secondary_DiagnosisCosts_Running3")
    )
