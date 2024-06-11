from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def EDVisits(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "ED Visits from Members in Household (Not Member)",
        when(~ col("`ED Visits`").isNull(), (col("`Plan Total ED Visits`") - col("`ED Visits`")))\
          .otherwise(col("`Plan Total ED Visits`"))
    )
