from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_4053(spark: SparkSession) -> DataFrame:
    
    colNames = ["Field2", "Field3"]
    data = [["00", "NO DIAGNOSITIC CATEGORY"], ["01", "NERVOUS SYSTEM"], ["02", "EYE"], ["03", "EAR, NOSE, AND THROAT"],
            ["04", "RESPIRATORY SYSTEM"], ["05", "CIRCULATORY SYSTEM"], ["06", "DIGESTIVE SYSTEM"],
            ["07", "HEPATOBILARY SYSTEM"], ["08", "MUSCULOSKETAL SYSTEM"],
            ["09", "SKIN SUBCUTANEOUS TISSUE AND BREAST"], ["10", "ENDOCRINE, NUTRITIONAL AND METABOLIC"],
            ["11", "KIDNEY AND URINARY TRACT"], ["12", "MALE REPRODUCTIVE SYSTEM"],
            ["13", "FEMALE REPRODUCTIVE SYSTEM"], ["14", "PREGNANCY, CHILDBIRTH AND PUERPERIUM"],
            ["15", "NEWBORNS AND OTHER NEONATES"], ["16", "BLOOD AND BLOOD FORMING ORGANS"],
            ["17", "MYELOPROLIFERATIVE AND NEOPLASMS"], ["18", "INFECTIOUS AND PARASITIC DISEASES"],
            ["19", "MENTAL ILLNESS"], ["20", "ALCOHOL/DRUG USE AND DISORDERS"],
            ["21", "INJURIES, POISONINGS AND DRUG EFFECTS"], ["22", "BURNS"],
            ["23", "FACTORS INFLUENCING HEALTH STATUS"], ["24", "MULTIPLE SIGNIFICANT TRAUMA"],
            ["25", "AIDS/HIV"], ["99", "NO DIAGNOSTIC CATEGORY"], ["NA", "NOT APPLICABLE"],
            ["UNK", "UNKNOWN"]]
    rows = [Row(*row) for row in data]
    schema = StructType([
            StructField("Field2", StringType(), nullable = True),
         StructField("Field3", StringType(), nullable = True)

    ])
    out0 = spark.createDataFrame(rows, schema)

    return out0
