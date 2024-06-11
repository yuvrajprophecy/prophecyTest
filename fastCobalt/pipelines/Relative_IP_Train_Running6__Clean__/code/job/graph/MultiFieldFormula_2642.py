from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_2642(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(col("Primary_Diagnosis_Cost").isNull(), lit(0))\
          .otherwise(col("Primary_Diagnosis_Cost"))\
          .alias("Primary_Diagnosis_Cost"), 
        when(col("Secondary_Diagnosis_Cost").isNull(), lit(0))\
          .otherwise(col("Secondary_Diagnosis_Cost"))\
          .alias("Secondary_Diagnosis_Cost"), 
        when(col("Tertiary_Diagnosis_Cost").isNull(), lit(0))\
          .otherwise(col("Tertiary_Diagnosis_Cost"))\
          .alias("Tertiary_Diagnosis_Cost"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("Tertiary_Diagnosis"), 
        col("Primary_Diagnosis"), 
        col("Secondary_Diagnosis")
    )
