from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_959(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Diagnosis", call_spark_fcn("string_substring", col("DIAG_CD"), lit(0), lit(3)))\
        .withColumn("Emergent Ind (OG)", when((col("noner").cast(DoubleType()) >= lit(0.5)), lit(1)).otherwise(lit(0)))\
        .withColumn("Emergent Ind (Proposed)", when(((col("noner") + col("epct")).cast(DoubleType()) >= lit(0.5)), lit(1)).otherwise(lit(0)))
