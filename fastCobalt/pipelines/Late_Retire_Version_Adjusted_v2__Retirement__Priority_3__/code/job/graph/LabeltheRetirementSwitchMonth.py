from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def LabeltheRetirementSwitchMonth(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Switch to Retirement",
          when(
              (
                (col("`Product Category_lag1`").cast(StringType()) != lit("Retire Prod"))
                & (col("`Product Category`").cast(StringType()) == lit("Retire Prod"))
              ), 
              lit(1)
            )\
            .otherwise(lit(0))
        )\
        .drop("Product Category_lag1")
