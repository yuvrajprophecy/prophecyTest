from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_782(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Secondary_Rx_Running3",
          concat(
            concat(concat(concat(col("Secondary_RX"), lit("; ")), col("Secondary_RX_lag1")), lit("; ")), 
            col("Secondary_RX_lag2")
          )
        )\
        .drop("Secondary_RX_lag1")\
        .drop("Secondary_RX_lag2")
