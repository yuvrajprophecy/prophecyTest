from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_2659_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.YEARMONTH") == col("in1.YEARMONTH"))),
          "inner"
        )\
        .select(col("in0.Secondary_Diagnosis_Cost").alias("Secondary_Diagnosis_Cost"), col("in1.OfLast6_Main_Diag").alias("OfLast6_Main_Diag"), col("in0.Tertiary_Diagnosis_Cost").alias("Tertiary_Diagnosis_Cost"), col("in1.OfLast6_Main_Diag_Cost").alias("OfLast6_Main_Diag_Cost"), col("in0.Primary_Diagnosis").alias("Primary_Diagnosis"), col("in0.Secondary_Diagnosis").alias("Secondary_Diagnosis"), col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.Primary_Diagnosis_Cost").alias("Primary_Diagnosis_Cost"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.Tertiary_Diagnosis").alias("Tertiary_Diagnosis"))
