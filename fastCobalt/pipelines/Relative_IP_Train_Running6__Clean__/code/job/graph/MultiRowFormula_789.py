from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_789(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Tertiary_Diagnosis_Running3",
          concat(
            concat(concat(concat(col("Tertiary_Diagnosis"), lit("; ")), col("Tertiary_Diagnosis_lag1")), lit("; ")), 
            col("Tertiary_Diagnosis_lag2")
          )
        )\
        .drop("Tertiary_Diagnosis_lag1")\
        .drop("Tertiary_Diagnosis_lag2")
