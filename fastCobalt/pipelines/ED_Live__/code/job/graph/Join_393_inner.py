from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_393_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.Week") == col("in1.Week"))),
          "inner"
        )\
        .select(col("in0.Concat_Diagnosis").alias("Diagnoses"), col("in1.Max_injury").alias("injury"), col("in1.Max_psych").alias("psych"), col("in0.Week").alias("Week"), col("in1.`Avg_ER Severity`").alias("Non Emergent Likelihood"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in1.Max_alcohol").alias("alcohol"), col("in1.Max_drug").alias("drug"))
