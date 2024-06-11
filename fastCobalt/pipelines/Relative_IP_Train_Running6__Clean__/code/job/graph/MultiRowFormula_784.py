from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_784(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Primary_RxCosts_Running3",
          ((col("Primary_RX_Cost") + col("Primary_RX_Cost_lag1")) + col("Primary_RX_Cost_lag2"))
        )\
        .drop("Primary_RX_Cost_lag1")\
        .drop("Primary_RX_Cost_lag2")
