from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4090_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY"))
            & (col("in0.`Received Date`") == col("in1.`Received Date`"))
          ),
          "fullouter"
        )\
        .select(col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in1.`Vision Member Pay`").alias("Vision Member Pay"), col("in0.`Unkown Drug Tier`").alias("Unkown Drug Tier"), col("in0.`Drug Tier 3`").alias("Drug Tier 3"), col("in0.`Received Date`").alias("Received Date"), col("in0.`Drug Tier 2`").alias("Drug Tier 2"), col("in0.`Generic Drug Counts`").alias("Generic Drug Counts"), col("in0.`Specialty Drug Counts`").alias("Specialty Drug Counts"), col("in0.`RX Member Pay`").alias("RX Member Pay"), col("in0.`Drug Tier 1`").alias("Drug Tier 1"), col("in1.`Vision EYE`").alias("Vision EYE"), col("in1.`Vision FACTORS_INFLUENCING_HEALTH_STATUS`").alias("Vision FACTORS_INFLUENCING_HEALTH_STATUS"), col("in0.`Distinct NDC Codes`").alias("Distinct NDC Codes"), col("in0.`Distinct Diagnosis Types`").alias("Distinct Diagnosis Types"), col("in0.`Mail Order Drug Counts`").alias("Mail Order Drug Counts"), col("in1.`Vision NO_DIAGNOSTIC_CATEGORY`").alias("Vision NO_DIAGNOSTIC_CATEGORY"), col("in1.`Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC`").alias("Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC"))
