from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_4354_window(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Target Forecasted_lead1",
          lead(col("`Target Forecasted`"), 1)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn(
          "Target Forecasted_lead2",
          lead(col("`Target Forecasted`"), 2)\
            .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc()))
        )\
        .withColumn("Target Forecasted_lead3", lead(col("`Target Forecasted`"), 3)\
        .over(Window.partitionBy(col("MBR_INDV_BE_KEY")).orderBy(col("MBR_INDV_BE_KEY").asc())))
