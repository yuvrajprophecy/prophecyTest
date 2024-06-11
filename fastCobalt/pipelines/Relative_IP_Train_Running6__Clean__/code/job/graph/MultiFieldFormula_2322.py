from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_2322(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(col("IP_PreviousMonth").isNull(), lit(0)).otherwise(col("IP_PreviousMonth")).alias("IP_PreviousMonth"), 
        when(col("IP_Running3").isNull(), lit(0)).otherwise(col("IP_Running3")).alias("IP_Running3"), 
        when(col("IP_Running6").isNull(), lit(0)).otherwise(col("IP_Running6")).alias("IP_Running6"), 
        when(col("IP_Running3_Max").isNull(), lit(0)).otherwise(col("IP_Running3_Max")).alias("IP_Running3_Max"), 
        when(col("IP_Running6_Max").isNull(), lit(0)).otherwise(col("IP_Running6_Max")).alias("IP_Running6_Max"), 
        col("MEMBER_AGE2"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_SK"), 
        col("Relative_IP_Month"), 
        col("MEMBER_AGE"), 
        col("First_IP"), 
        col("IP_Month_Number"), 
        col("MBR_GNDR_CD"), 
        col("TOT_MONTHS"), 
        col("RecordID"), 
        col("IP_Record")
    )
