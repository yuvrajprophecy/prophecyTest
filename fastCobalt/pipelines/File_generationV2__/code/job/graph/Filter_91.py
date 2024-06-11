from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_91(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          ((col("UnitName").cast(StringType()) == lit("BCBS KC PC")) | col("ProductName").contains(lit("KC")))
          & ~ array_contains(array(lit("Saturday"), lit("Sunday")), col("`Day of Week`").cast(StringType()))
        )
    )
