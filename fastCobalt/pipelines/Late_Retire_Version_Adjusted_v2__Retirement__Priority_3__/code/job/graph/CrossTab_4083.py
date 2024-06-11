from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_4083(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("`Received Date`").alias("Received Date"))
    df2 = df1.pivot(
        "`Diagnosis Type`",
        ["FEMALE_REPRODUCTIVE_SYSTEM",  "BLOOD_AND_BLOOD_FORMING_ORGANS",  "NEWBORNS_AND_OTHER_NEONATES",          "INFECTIOUS_AND_PARASITIC_DISEASES",  "RESPIRATORY_SYSTEM",  "EYE",  "EAR__NOSE__AND_THROAT",          "NO_DIAGNOSTIC_CATEGORY",  "NOT_APPLICABLE",  "MALE_REPRODUCTIVE_SYSTEM",          "ENDOCRINE__NUTRITIONAL_AND_METABOLIC",  "MENTAL_ILLNESS",  "MUSCULOSKETAL_SYSTEM",          "INJURIES__POISONINGS_AND_DRUG_EFFECTS",  "SKIN_SUBCUTANEOUS_TISSUE_AND_BREAST",  "BURNS",          "PREGNANCY__CHILDBIRTH_AND_PUERPERIUM",  "FACTORS_INFLUENCING_HEALTH_STATUS",  "NERVOUS_SYSTEM",          "KIDNEY_AND_URINARY_TRACT",  "MYELOPROLIFERATIVE_AND_NEOPLASMS",  "AIDS_HIV",  "HEPATOBILARY_SYSTEM",          "ALCOHOL_DRUG_USE_AND_DISORDERS",  "DIGESTIVE_SYSTEM",  "CIRCULATORY_SYSTEM",  "UNKNOWN"]
    )

    return df2.agg(sum(col("counter")).alias("counter"))
