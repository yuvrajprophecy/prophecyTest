from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_813(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        array_contains(
          array(lit(- 1), lit(- 3), lit(- 6), lit(- 9), lit(- 12)), 
          col("Relative_IP_Month").cast(DoubleType())
        )
    )
