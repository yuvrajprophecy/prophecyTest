from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_3752(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Male Percentage`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Male Percentage`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Male Percentage`"))\
          .alias("Male Percentage"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Female Percentage`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Female Percentage`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Female Percentage`"))\
          .alias("Female Percentage"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Average Age Males`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Average Age Males`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Average Age Males`"))\
          .alias("Average Age Males"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Average Age Females`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Average Age Females`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Average Age Females`"))\
          .alias("Average Age Females"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("SUM_CCI").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("SUM_CCI") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("SUM_CCI"))\
          .alias("SUM_CCI"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Drug Tier 1`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Drug Tier 1`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Drug Tier 1`"))\
          .alias("Drug Tier 1"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Drug Tier 2`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Drug Tier 2`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Drug Tier 2`"))\
          .alias("Drug Tier 2"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Drug Tier 3`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Drug Tier 3`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Drug Tier 3`"))\
          .alias("Drug Tier 3"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Distinct NDC Codes`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Distinct NDC Codes`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Distinct NDC Codes`"))\
          .alias("Distinct NDC Codes"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Distinct Diagnosis Types`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Distinct Diagnosis Types`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Distinct Diagnosis Types`"))\
          .alias("Distinct Diagnosis Types"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Specialty Drug Counts`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Specialty Drug Counts`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Specialty Drug Counts`"))\
          .alias("Specialty Drug Counts"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Generic Drug Counts`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Generic Drug Counts`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Generic Drug Counts`"))\
          .alias("Generic Drug Counts"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Mail Order Drug Counts`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Mail Order Drug Counts`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Mail Order Drug Counts`"))\
          .alias("Mail Order Drug Counts"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC`"))\
          .alias("Vision ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Vision EYE`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Vision EYE`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Vision EYE`"))\
          .alias("Vision EYE"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Vision FACTORS_INFLUENCING_HEALTH_STATUS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Vision FACTORS_INFLUENCING_HEALTH_STATUS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Vision FACTORS_INFLUENCING_HEALTH_STATUS`"))\
          .alias("Vision FACTORS_INFLUENCING_HEALTH_STATUS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Vision Member Pay`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Vision Member Pay`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Vision Member Pay`"))\
          .alias("Vision Member Pay"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`RX Member Pay`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`RX Member Pay`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`RX Member Pay`"))\
          .alias("RX Member Pay"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical ALCOHOL_DRUG_USE_AND_DISORDERS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical ALCOHOL_DRUG_USE_AND_DISORDERS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical ALCOHOL_DRUG_USE_AND_DISORDERS`"))\
          .alias("Medical ALCOHOL_DRUG_USE_AND_DISORDERS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical BLOOD_AND_BLOOD_FORMING_ORGANS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical BLOOD_AND_BLOOD_FORMING_ORGANS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical BLOOD_AND_BLOOD_FORMING_ORGANS`"))\
          .alias("Medical BLOOD_AND_BLOOD_FORMING_ORGANS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical BURNS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical BURNS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical BURNS`"))\
          .alias("Medical BURNS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical CIRCULATORY_SYSTEM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical CIRCULATORY_SYSTEM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical CIRCULATORY_SYSTEM`"))\
          .alias("Medical CIRCULATORY_SYSTEM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical DIGESTIVE_SYSTEM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical DIGESTIVE_SYSTEM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical DIGESTIVE_SYSTEM`"))\
          .alias("Medical DIGESTIVE_SYSTEM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical EAR__NOSE__AND_THROAT`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical EAR__NOSE__AND_THROAT`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical EAR__NOSE__AND_THROAT`"))\
          .alias("Medical EAR__NOSE__AND_THROAT"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC`"))\
          .alias("Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical EYE`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical EYE`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical EYE`"))\
          .alias("Medical EYE"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical FACTORS_INFLUENCING_HEALTH_STATUS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical FACTORS_INFLUENCING_HEALTH_STATUS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical FACTORS_INFLUENCING_HEALTH_STATUS`"))\
          .alias("Medical FACTORS_INFLUENCING_HEALTH_STATUS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical FEMALE_REPRODUCTIVE_SYSTEM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical FEMALE_REPRODUCTIVE_SYSTEM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical FEMALE_REPRODUCTIVE_SYSTEM`"))\
          .alias("Medical FEMALE_REPRODUCTIVE_SYSTEM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical HEPATOBILARY_SYSTEM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical HEPATOBILARY_SYSTEM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical HEPATOBILARY_SYSTEM`"))\
          .alias("Medical HEPATOBILARY_SYSTEM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical INFECTIOUS_AND_PARASITIC_DISEASES`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical INFECTIOUS_AND_PARASITIC_DISEASES`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical INFECTIOUS_AND_PARASITIC_DISEASES`"))\
          .alias("Medical INFECTIOUS_AND_PARASITIC_DISEASES"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS`"))\
          .alias("Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical KIDNEY_AND_URINARY_TRACT`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical KIDNEY_AND_URINARY_TRACT`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical KIDNEY_AND_URINARY_TRACT`"))\
          .alias("Medical KIDNEY_AND_URINARY_TRACT"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical MALE_REPRODUCTIVE_SYSTEM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical MALE_REPRODUCTIVE_SYSTEM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical MALE_REPRODUCTIVE_SYSTEM`"))\
          .alias("Medical MALE_REPRODUCTIVE_SYSTEM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical MENTAL_ILLNESS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical MENTAL_ILLNESS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical MENTAL_ILLNESS`"))\
          .alias("Medical MENTAL_ILLNESS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical MUSCULOSKETAL_SYSTEM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical MUSCULOSKETAL_SYSTEM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical MUSCULOSKETAL_SYSTEM`"))\
          .alias("Medical MUSCULOSKETAL_SYSTEM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical MYELOPROLIFERATIVE_AND_NEOPLASMS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical MYELOPROLIFERATIVE_AND_NEOPLASMS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical MYELOPROLIFERATIVE_AND_NEOPLASMS`"))\
          .alias("Medical MYELOPROLIFERATIVE_AND_NEOPLASMS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical NERVOUS_SYSTEM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical NERVOUS_SYSTEM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical NERVOUS_SYSTEM`"))\
          .alias("Medical NERVOUS_SYSTEM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical NEWBORNS_AND_OTHER_NEONATES`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical NEWBORNS_AND_OTHER_NEONATES`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical NEWBORNS_AND_OTHER_NEONATES`"))\
          .alias("Medical NEWBORNS_AND_OTHER_NEONATES"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM`"))\
          .alias("Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical RESPIRATORY_SYSTEM`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical RESPIRATORY_SYSTEM`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical RESPIRATORY_SYSTEM`"))\
          .alias("Medical RESPIRATORY_SYSTEM"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`"))\
          .alias("Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Med Member Pay`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Med Member Pay`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Med Member Pay`"))\
          .alias("Med Member Pay"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Medical AIDS_HIV`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Medical AIDS_HIV`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Medical AIDS_HIV`"))\
          .alias("Medical AIDS_HIV"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Dental ALCOHOL_DRUG_USE_AND_DISORDERS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Dental ALCOHOL_DRUG_USE_AND_DISORDERS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Dental ALCOHOL_DRUG_USE_AND_DISORDERS`"))\
          .alias("Dental ALCOHOL_DRUG_USE_AND_DISORDERS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Dental EAR__NOSE__AND_THROAT`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Dental EAR__NOSE__AND_THROAT`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Dental EAR__NOSE__AND_THROAT`"))\
          .alias("Dental EAR__NOSE__AND_THROAT"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Dental FACTORS_INFLUENCING_HEALTH_STATUS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Dental FACTORS_INFLUENCING_HEALTH_STATUS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Dental FACTORS_INFLUENCING_HEALTH_STATUS`"))\
          .alias("Dental FACTORS_INFLUENCING_HEALTH_STATUS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Dental MENTAL_ILLNESS`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Dental MENTAL_ILLNESS`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Dental MENTAL_ILLNESS`"))\
          .alias("Dental MENTAL_ILLNESS"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Dental SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Dental SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Dental SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST`"))\
          .alias("Dental SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Dental Dental Member Pay`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Dental Dental Member Pay`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Dental Dental Member Pay`"))\
          .alias("Dental Dental Member Pay"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("Emergency_Room").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("Emergency_Room") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("Emergency_Room"))\
          .alias("Emergency_Room"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("Inpatient").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("Inpatient") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("Inpatient"))\
          .alias("Inpatient"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("Outpatient").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("Outpatient") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("Outpatient"))\
          .alias("Outpatient"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Number of Winterizing Procedures`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Number of Winterizing Procedures`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Number of Winterizing Procedures`"))\
          .alias("Number of Winterizing Procedures"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Dental Member Pay`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Dental Member Pay`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Dental Member Pay`"))\
          .alias("Dental Member Pay"), 
        when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(1)), 
            (col("`Depression Related Claims`").cast(IntegerType()) / lit(3))
          )\
          .when(
            ((col("`weight field`").cast(IntegerType()) % lit(3)).cast(IntegerType()) == lit(2)), 
            (col("`Depression Related Claims`") * (lit(2) / lit(3).cast(IntegerType())))
          )\
          .otherwise(col("`Depression Related Claims`"))\
          .alias("Depression Related Claims"), 
        col("YEARMONTH"), 
        col("MBR_INDV_BE_KEY"), 
        col("MBR_RELSHP_NM"), 
        col("`Target Forecasted`").alias("Target Forecasted"), 
        col("Avg_PCB"), 
        col("Avg_BCARE"), 
        col("Avg_HPEXTRNL"), 
        col("YMD"), 
        col("`Retired With Blue`").alias("Retired With Blue"), 
        col("Avg_BLUESELECT_"), 
        col("SUB_MBR_SK"), 
        col("`Spouse Age`").alias("Spouse Age"), 
        col("previous"), 
        col("MBR_HOME_ADDR_ZIP_CD_5"), 
        col("PCP_FLAG"), 
        col("`Min_Dependent Age`").alias("Min_Dependent Age"), 
        col("GRP_ID"), 
        col("Avg_PC"), 
        col("HOST_MBR_IN"), 
        col("AGE"), 
        col("MBR_DSBLTY_IN"), 
        col("EXPRNC_CAT_CD"), 
        col("FUND_CAT_CD"), 
        col("CLS_PLN_DESC"), 
        col("PROD_SH_NM_DLVRY_METH_CD"), 
        col("Members_on_Policy"), 
        col("PROD_SH_NM"), 
        col("GRP_TOT_EMPL_CT"), 
        col("`Apt Ind`").alias("Apt Ind"), 
        col("SPIRA_BNF_ID"), 
        col("MBR_GNDR_CD"), 
        col("`Member Months`").alias("Member Months"), 
        col("MBR_ENR_COBRA_IN"), 
        col("`Product Category`").alias("Product Category"), 
        col("Avg_BLUE_SELECT"), 
        col("GRP_MKT_SIZE_CAT_NM"), 
        col("`Group Months`").alias("Group Months"), 
        col("PRNT_GRP_SIC_NACIS_CD"), 
        col("`High Deductible Ind`").alias("High Deductible Ind"), 
        col("MBR_HOME_ADDR_ST_CD"), 
        col("FNCL_LOB_CD"), 
        col("FNCL_MKT_SEG_NM"), 
        col("`weight field`").alias("weight field"), 
        col("GRP_ZIP_CD_5"), 
        col("`Spouse Retired`").alias("Spouse Retired"), 
        col("Avg_BLUE_ACCESS"), 
        col("Avg_PCBEXTRNL")
    )
