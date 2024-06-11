from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_971(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`ED Prediction Score`").alias("ED Prediction Score"), 
        col("FREQUENT_FLYER"), 
        col("`ED Prediction Value`").alias("ED Prediction Value"), 
        col("`Member Individual Business Entity Key`").alias("Member Individual Business Entity Key")
    )
