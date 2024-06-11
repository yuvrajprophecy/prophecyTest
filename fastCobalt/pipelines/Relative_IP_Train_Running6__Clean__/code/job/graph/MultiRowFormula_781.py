from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_781(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Primary_Rx_Running3",
          concat(
            concat(concat(concat(col("Primary_RX"), lit("; ")), col("Primary_RX_lag1")), lit("; ")), 
            col("Primary_RX_lag2")
          )
        )\
        .drop("Primary_RX_lag1")\
        .drop("Primary_RX_lag2")
