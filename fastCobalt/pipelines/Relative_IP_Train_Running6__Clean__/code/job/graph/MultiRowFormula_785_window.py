from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_785_window(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Tertiary_RX_Cost_lag1",
          lag(col("Tertiary_RX_Cost"), 1)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn("Tertiary_RX_Cost_lag2", lag(col("Tertiary_RX_Cost"), 2)\
        .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc())))
