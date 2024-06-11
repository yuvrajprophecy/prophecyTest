from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4095_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY"))
            & (col("in0.`Received Date`") == col("in1.`Received Date`"))
          ),
          "inner"
        )\
        .select(col("in0.EYE").alias("EYE"), col("in0.MENTAL_ILLNESS").alias("MENTAL_ILLNESS"), col("in0.EAR__NOSE__AND_THROAT").alias("EAR__NOSE__AND_THROAT"), col("in0.ALCOHOL_DRUG_USE_AND_DISORDERS").alias("ALCOHOL_DRUG_USE_AND_DISORDERS"), col("in0.FACTORS_INFLUENCING_HEALTH_STATUS").alias("FACTORS_INFLUENCING_HEALTH_STATUS"), col("in0.NOT_APPLICABLE").alias("NOT_APPLICABLE"), col("in0.SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST").alias("SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), col("in1.Sum_MemberPay").alias("Dental Member Pay"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.`Received Date`").alias("Received Date"), col("in0.NO_DIAGNOSTIC_CATEGORY").alias("NO_DIAGNOSTIC_CATEGORY"))
