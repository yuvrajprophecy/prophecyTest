from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DynamicRename_4089(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MBR_INDV_BE_KEY"), 
        col("`Med Member Pay`").alias("Med Member Pay"), 
        col("FEMALE_REPRODUCTIVE_SYSTEM").alias("Medical FEMALE_REPRODUCTIVE_SYSTEM"), 
        col("BLOOD_AND_BLOOD_FORMING_ORGANS").alias("Medical BLOOD_AND_BLOOD_FORMING_ORGANS"), 
        col("NEWBORNS_AND_OTHER_NEONATES").alias("Medical NEWBORNS_AND_OTHER_NEONATES"), 
        col("INFECTIOUS_AND_PARASITIC_DISEASES").alias("Medical INFECTIOUS_AND_PARASITIC_DISEASES"), 
        col("RESPIRATORY_SYSTEM").alias("Medical RESPIRATORY_SYSTEM"), 
        col("EYE").alias("Medical EYE"), 
        col("EAR__NOSE__AND_THROAT").alias("Medical EAR__NOSE__AND_THROAT"), 
        col("`Received Date`").alias("Medical Received Date"), 
        col("NO_DIAGNOSTIC_CATEGORY").alias("Medical NO_DIAGNOSTIC_CATEGORY"), 
        col("NOT_APPLICABLE").alias("Medical NOT_APPLICABLE"), 
        col("MALE_REPRODUCTIVE_SYSTEM").alias("Medical MALE_REPRODUCTIVE_SYSTEM"), 
        col("ENDOCRINE__NUTRITIONAL_AND_METABOLIC").alias("Medical ENDOCRINE__NUTRITIONAL_AND_METABOLIC"), 
        col("MENTAL_ILLNESS").alias("Medical MENTAL_ILLNESS"), 
        col("MUSCULOSKETAL_SYSTEM").alias("Medical MUSCULOSKETAL_SYSTEM"), 
        col("INJURIES__POISONINGS_AND_DRUG_EFFECTS").alias("Medical INJURIES__POISONINGS_AND_DRUG_EFFECTS"), 
        col("SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST").alias("Medical SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST"), 
        col("BURNS").alias("Medical BURNS"), 
        col("PREGNANCY__CHILDBIRTH_AND_PUERPERIUM").alias("Medical PREGNANCY__CHILDBIRTH_AND_PUERPERIUM"), 
        col("FACTORS_INFLUENCING_HEALTH_STATUS").alias("Medical FACTORS_INFLUENCING_HEALTH_STATUS"), 
        col("NERVOUS_SYSTEM").alias("Medical NERVOUS_SYSTEM"), 
        col("KIDNEY_AND_URINARY_TRACT").alias("Medical KIDNEY_AND_URINARY_TRACT"), 
        col("MYELOPROLIFERATIVE_AND_NEOPLASMS").alias("Medical MYELOPROLIFERATIVE_AND_NEOPLASMS"), 
        col("AIDS_HIV").alias("Medical AIDS_HIV"), 
        col("HEPATOBILARY_SYSTEM").alias("Medical HEPATOBILARY_SYSTEM"), 
        col("ALCOHOL_DRUG_USE_AND_DISORDERS").alias("Medical ALCOHOL_DRUG_USE_AND_DISORDERS"), 
        col("DIGESTIVE_SYSTEM").alias("Medical DIGESTIVE_SYSTEM"), 
        col("CIRCULATORY_SYSTEM").alias("Medical CIRCULATORY_SYSTEM"), 
        col("UNKNOWN").alias("Medical UNKNOWN")
    )
