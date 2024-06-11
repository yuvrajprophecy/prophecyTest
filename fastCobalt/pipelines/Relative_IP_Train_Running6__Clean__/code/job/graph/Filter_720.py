from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_720(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (call_spark_fcn("string_substring", col("DIAG_CD"), lit(0), lit(1)).cast(StringType()) == lit("E"))
          | array_contains(
            array(
              lit("Z3"), 
              lit("O3"), 
              lit("O0"), 
              lit("Z0"), 
              lit("M1"), 
              lit("Z2"), 
              lit("Z1"), 
              lit("R1"), 
              lit("M4"), 
              lit("F3"), 
              lit("O2")
            ), 
            call_spark_fcn("string_substring", col("DIAG_CD"), lit(0), lit(2)).cast(StringType())
          )
        )
    )
