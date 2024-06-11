from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_4092_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY"))
            & (col("in0.`Received Date`") == col("in1.`Medical Received Date`"))
          ),
          "fullouter"
        )\
        .select(col("in1.`Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS`")\
        .alias("Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS"), col("in1.`Medical FACTORS_INFLUENCING_HEALTH_STATUS`").alias("Medical FACTORS_INFLUENCING_HEALTH_STATUS"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.`Vision Member Pay`").alias("Vision Member Pay"), col("in0.`Unkown Drug Tier`").alias("Unkown Drug Tier"), col("in1.`Medical BURNS`").alias("Medical BURNS"), col("in0.`Drug Tier 3`").alias("Drug Tier 3"), col("in1.`Medical RESPIRATORY_SYSTEM`").alias("Medical RESPIRATORY_SYSTEM"), col("in1.`Med Member Pay`").alias("Med Member Pay"), col("in1.`Medical FEMALE_REPRODUCTIVE_SYSTEM`").alias("Medical FEMALE_REPRODUCTIVE_SYSTEM"), col("in1.`Medical MALE_REPRODUCTIVE_SYSTEM`").alias("Medical MALE_REPRODUCTIVE_SYSTEM"), col("in0.`Received Date`").alias("Received Date"), col("in1.`Medical NOT_APPLICABLE`").alias("Medical NOT_APPLICABLE"), col("in1.`Medical AIDS_HIV`").alias("Medical AIDS_HIV"), col("in1.`Medical MENTAL_ILLNESS`").alias("Medical MENTAL_ILLNESS"), col("in1.`Medical KIDNEY_AND_URINARY_TRACT`").alias("Medical KIDNEY_AND_URINARY_TRACT"), col("in1.`Medical MYELOPROLIFERATIVE_AND_NEOPLASMS`").alias("Medical MYELOPROLIFERATIVE_AND_NEOPLASMS"), col("in1.`Medical EAR__NOSE__AND_THROAT`").alias("Medical EAR__NOSE__AND_THROAT"), col("in1.`Medical BLOOD_AND_BLOOD_FORMING_ORGANS`").alias("Medical BLOOD_AND_BLOOD_FORMING_ORGANS"), col("in0.`Drug Tier 2`").alias("Drug Tier 2"), col("in1.`Medical Received Date`").alias("Medical Received Date"), col("in0.`Generic Drug Counts`").alias("Generic Drug Counts"), col("in1.`Medical NO_DIAGNOSTIC_CATEGORY`").alias("Medical NO_DIAGNOSTIC_CATEGORY"), col("in0.`Specialty Drug Counts`").alias("Specialty Drug Counts"), col("in1.`Medical DIGESTIVE_SYSTEM`").alias("Medical DIGESTIVE_SYSTEM"), col("in1.`Medical INFECTIOUS_AND_PARASITIC_DISEASES`").alias("Medical INFECTIOUS_AND_PARASITIC_DISEASES"), col("in0.`RX Member Pay`").alias("RX Member Pay"), col("in0.`Drug Tier 1`").alias("Drug Tier 1"), col("in0.`Vision EYE`").alias("Vision EYE"), col("in1.`Medical UNKNOWN`").alias("Medical UNKNOWN"), col("in0.`Vision FACTORS_INFLUENCING_HEALTH_STATUS`").alias("Vision FACTORS_INFLUENCING_HEALTH_STATUS"), col("in0.`Distinct NDC Codes`").alias("Distinct NDC Codes"), col("in1.`Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC`")\
        .alias("Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), col("in1.`Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`").alias("Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), col("in0.`Distinct Diagnosis Types`").alias("Distinct Diagnosis Types"), col("in1.`Medical HEPATOBILARY_SYSTEM`").alias("Medical HEPATOBILARY_SYSTEM"), col("in0.`Mail Order Drug Counts`").alias("Mail Order Drug Counts"), col("in0.`Vision NO_DIAGNOSTIC_CATEGORY`").alias("Vision NO_DIAGNOSTIC_CATEGORY"), col("in1.`Medical EYE`").alias("Medical EYE"), col("in1.`Medical ALCOHOL_DRUG_USE_AND_DISORDERS`").alias("Medical ALCOHOL_DRUG_USE_AND_DISORDERS"), col("in1.`Medical MUSCULOSKETAL_SYSTEM`").alias("Medical MUSCULOSKETAL_SYSTEM"), col("in0.`Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC`").alias("Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), col("in1.`Medical NEWBORNS_AND_OTHER_NEONATES`").alias("Medical NEWBORNS_AND_OTHER_NEONATES"), col("in1.`Medical CIRCULATORY_SYSTEM`").alias("Medical CIRCULATORY_SYSTEM"), col("in1.`Medical NERVOUS_SYSTEM`").alias("Medical NERVOUS_SYSTEM"), col("in1.`Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM`")\
        .alias("Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM"))
