from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_790(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Primary_DiagnosisCosts_Running3",
          ((col("Primary_Diagnosis_Cost") + col("Primary_Diagnosis_Cost_lag1")) + col("Primary_Diagnosis_Cost_lag2"))
        )\
        .drop("Primary_Diagnosis_Cost_lag1")\
        .drop("Primary_Diagnosis_Cost_lag2")
