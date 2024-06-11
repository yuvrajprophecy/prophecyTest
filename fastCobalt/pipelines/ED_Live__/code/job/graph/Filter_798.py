from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_798(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          ((col("CountDistinct_Week").cast(IntegerType()) == lit(26)) & (col("Last_Week") <= expr("date_add(current_date(), -7)")))
          & (col("Last_Week") > expr("date_add(current_date(), -14)"))
        )
    )
