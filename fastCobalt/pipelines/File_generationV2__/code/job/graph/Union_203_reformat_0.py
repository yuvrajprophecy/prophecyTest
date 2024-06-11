from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_203_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Time"), 
        col("Date"), 
        lit(None).cast(StringType()).alias("Time Distributed Hours Upper"), 
        lit(None).cast(DoubleType()).alias("Calls Lower"), 
        lit(None).cast(StringType()).alias("Time Distributed Calls Upper"), 
        lit(None).cast(DoubleType()).alias("Upper Bound (95% CI)"), 
        lit(None).cast(StringType()).alias("Time Distributed Hours Lower"), 
        lit(None).cast(StringType()).alias("Prct"), 
        lit(None).cast(StringType()).alias("Time Distributed Calls Lower"), 
        lit(None).cast(StringType()).alias("Time Distributed Hours"), 
        lit(None).cast(DoubleType()).alias("Anticipated Hours"), 
        lit(None).cast(DoubleType()).alias("Calls Upper"), 
        lit(None).cast(StringType()).alias("Time Distributed Calls"), 
        lit(None).cast(DoubleType()).alias("Calls"), 
        lit(None).cast(DoubleType()).alias("Lower Bound (95% CI)")
    )
