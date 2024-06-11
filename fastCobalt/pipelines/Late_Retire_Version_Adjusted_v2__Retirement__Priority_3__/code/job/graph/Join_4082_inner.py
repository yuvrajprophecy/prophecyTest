from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4082_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
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
        .select(col("in1.Sum_SpecialtyIndicator").alias("Specialty Drug Counts"), col("in1.CountDistinct_NDC_SK").alias("Distinct NDC Codes"), col("in0.TIER1").alias("Drug Tier 1"), col("in1.Sum_MailOrder").alias("Mail Order Drug Counts"), col("in1.`CountDistinct_Diagnosis Type`").alias("Distinct Diagnosis Types"), col("in0.UNKTIER").alias("Unkown Drug Tier"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.`Received Date`").alias("Received Date"), col("in1.Sum_GenericIndicator").alias("Generic Drug Counts"), col("in1.Sum_MemberPay").alias("RX Member Pay"), col("in0.TIER2").alias("Drug Tier 2"), col("in0.TIER3").alias("Drug Tier 3"))
