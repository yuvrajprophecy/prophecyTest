from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2605(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "CCI_6MoSum",
          (((((col("CCI") + col("CCI_lag1")) + col("CCI_lag2")) + col("CCI_lag3")) + col("CCI_lag4")) + col("CCI_lag5"))
        )\
        .drop("CCI_lag1")\
        .drop("CCI_lag2")\
        .drop("CCI_lag3")\
        .drop("CCI_lag4")\
        .drop("CCI_lag5")
