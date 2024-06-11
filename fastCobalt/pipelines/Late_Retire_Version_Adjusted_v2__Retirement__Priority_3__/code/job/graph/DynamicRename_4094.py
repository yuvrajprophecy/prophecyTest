from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DynamicRename_4094(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("`Dental Member Pay`").alias("Dental Dental Member Pay"), 
        col("EYE").alias("Dental EYE"), 
        col("EAR__NOSE__AND_THROAT").alias("Dental EAR__NOSE__AND_THROAT"), 
        col("`Received Date`").alias("Dental Received Date"), 
        col("NO_DIAGNOSTIC_CATEGORY").alias("Dental NO_DIAGNOSTIC_CATEGORY"), 
        col("NOT_APPLICABLE").alias("Dental NOT_APPLICABLE"), 
        col("MENTAL_ILLNESS").alias("Dental MENTAL_ILLNESS"), 
        col("SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST").alias("Dental SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), 
        col("FACTORS_INFLUENCING_HEALTH_STATUS").alias("Dental FACTORS_INFLUENCING_HEALTH_STATUS"), 
        col("ALCOHOL_DRUG_USE_AND_DISORDERS").alias("Dental ALCOHOL_DRUG_USE_AND_DISORDERS")
    )
