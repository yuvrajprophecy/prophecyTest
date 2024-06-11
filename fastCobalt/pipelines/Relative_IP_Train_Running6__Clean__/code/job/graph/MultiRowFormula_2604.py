from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2604(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "CCI_Growth",
          (
            (
              (
                (
                  ((col("CCI") - col("CCI_lag1")) * (lit(5) / lit(5).cast(IntegerType())))
                  + ((col("CCI_lag1") - col("CCI_lag2")) * (lit(4) / lit(5).cast(IntegerType())))
                )
                + ((col("CCI_lag2") - col("CCI_lag3")) * (lit(3) / lit(5).cast(IntegerType())))
              )
              + ((col("CCI_lag3") - col("CCI_lag4")) * (lit(2) / lit(5).cast(IntegerType())))
            )
            + ((col("CCI_lag4") - col("CCI_lag5")) * (lit(1) / lit(5).cast(IntegerType())))
          )
        )\
        .drop("CCI_lag1")\
        .drop("CCI_lag1")\
        .drop("CCI_lag2")\
        .drop("CCI_lag2")\
        .drop("CCI_lag3")\
        .drop("CCI_lag3")\
        .drop("CCI_lag4")\
        .drop("CCI_lag4")\
        .drop("CCI_lag5")
