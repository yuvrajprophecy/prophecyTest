from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_1032(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "ED Prediction Score",
          call_spark_fcn("string_substring", col("`ED Prediction Score`"), lit(3), length(col("`ED Prediction Score`")))
        )\
        .withColumn("ED Prediction Score", when((col("`ED Prediction Score`").cast(StringType()) == lit("Low")), lit("LOW"))\
        .when((col("`ED Prediction Score`").cast(StringType()) == lit("Medium")), lit("MODERATE"))\
        .when((col("`ED Prediction Score`").cast(StringType()) == lit("High")), lit("HIGH"))\
        .otherwise(lit("CRITICAL")))
