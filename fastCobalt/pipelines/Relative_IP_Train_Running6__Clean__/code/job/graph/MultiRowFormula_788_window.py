from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_788_window(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Secondary_Diagnosis_lag1",
          lag(col("Secondary_Diagnosis"), 1)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn("Secondary_Diagnosis_lag2", lag(col("Secondary_Diagnosis"), 2)\
        .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc())))
