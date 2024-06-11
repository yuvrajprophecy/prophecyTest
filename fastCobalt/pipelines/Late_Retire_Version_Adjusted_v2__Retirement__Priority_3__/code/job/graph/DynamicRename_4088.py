from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DynamicRename_4088(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("`Vision Member Pay`").alias("Vision Member Pay"), 
        col("`Received Date`").alias("Received Date"), 
        col("ENDOCRINE__NUTRITIONAL_AND_METABOLIC").alias("Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), 
        col("EYE").alias("Vision EYE"), 
        col("FACTORS_INFLUENCING_HEALTH_STATUS").alias("Vision FACTORS_INFLUENCING_HEALTH_STATUS"), 
        col("NO_DIAGNOSTIC_CATEGORY").alias("Vision NO_DIAGNOSTIC_CATEGORY")
    )
