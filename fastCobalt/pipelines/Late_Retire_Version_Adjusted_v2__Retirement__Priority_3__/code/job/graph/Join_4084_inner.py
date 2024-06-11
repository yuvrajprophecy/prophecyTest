from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4084_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
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
        .select(col("in0.EYE").alias("EYE"), col("in0.BLOOD_AND_BLOOD_FORMING_ORGANS").alias("BLOOD_AND_BLOOD_FORMING_ORGANS"), col("in0.MENTAL_ILLNESS").alias("MENTAL_ILLNESS"), col("in0.MYELOPROLIFERATIVE_AND_NEOPLASMS").alias("MYELOPROLIFERATIVE_AND_NEOPLASMS"), col("in0.EAR__NOSE__AND_THROAT").alias("EAR__NOSE__AND_THROAT"), col("in0.AIDS_HIV").alias("AIDS_HIV"), col("in0.FEMALE_REPRODUCTIVE_SYSTEM").alias("FEMALE_REPRODUCTIVE_SYSTEM"), col("in0.ALCOHOL_DRUG_USE_AND_DISORDERS").alias("ALCOHOL_DRUG_USE_AND_DISORDERS"), col("in0.NERVOUS_SYSTEM").alias("NERVOUS_SYSTEM"), col("in0.FACTORS_INFLUENCING_HEALTH_STATUS").alias("FACTORS_INFLUENCING_HEALTH_STATUS"), col("in0.NOT_APPLICABLE").alias("NOT_APPLICABLE"), col("in0.RESPIRATORY_SYSTEM").alias("RESPIRATORY_SYSTEM"), col("in0.MALE_REPRODUCTIVE_SYSTEM").alias("MALE_REPRODUCTIVE_SYSTEM"), col("in0.UNKNOWN").alias("UNKNOWN"), col("in0.DIGESTIVE_SYSTEM").alias("DIGESTIVE_SYSTEM"), col("in0.CIRCULATORY_SYSTEM").alias("CIRCULATORY_SYSTEM"), col("in0.NEWBORNS_AND_OTHER_NEONATES").alias("NEWBORNS_AND_OTHER_NEONATES"), col("in0.SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST").alias("SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), col("in0.KIDNEY_AND_URINARY_TRACT").alias("KIDNEY_AND_URINARY_TRACT"), col("in0.MUSCULOSKETAL_SYSTEM").alias("MUSCULOSKETAL_SYSTEM"), col("in1.Sum_MemberPay").alias("Med Member Pay"), col("in0.INJURIES__POISONINGS_AND_DRUG_EFFECTS").alias("INJURIES__POISONINGS_AND_DRUG_EFFECTS"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.`Received Date`").alias("Received Date"), col("in0.PREGNANCY__CHILDBIRTH_AND_PUERPERIUM").alias("PREGNANCY__CHILDBIRTH_AND_PUERPERIUM"), col("in0.NO_DIAGNOSTIC_CATEGORY").alias("NO_DIAGNOSTIC_CATEGORY"), col("in0.HEPATOBILARY_SYSTEM").alias("HEPATOBILARY_SYSTEM"), col("in0.BURNS").alias("BURNS"), col("in0.ENDOCRINE__NUTRITIONAL_AND_METABOLIC").alias("ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), col("in0.INFECTIOUS_AND_PARASITIC_DISEASES").alias("INFECTIOUS_AND_PARASITIC_DISEASES"))
