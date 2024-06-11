from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_544(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("First2", call_spark_fcn("string_substring", col("DIAG_CD"), lit(0), lit(2)))\
        .withColumn(
          "Third",
          call_spark_fcn(
            "string_substring", 
            call_spark_fcn("string_substring", col("DIAG_CD"), lit(0), lit(3)), 
            (lit(- 1) * lit(1).cast(IntegerType())), 
            lit(1)
          )
        )\
        .withColumn(
          "Fourth",
          call_spark_fcn(
            "string_substring", 
            call_spark_fcn("string_substring", col("DIAG_CD"), lit(0), lit(4)), 
            (lit(- 1) * lit(1).cast(IntegerType())), 
            lit(1)
          )
        )\
        .withColumn("YEARMONTH", call_spark_fcn("string_substring", col("CLM_LN_SVC_STRT_DT_SK"), lit(0), lit(7)))\
        .withColumn("Count", lit(1))\
        .withColumn("Third", when(col("Third").isNull(), lit("*")).otherwise(col("Third")))\
        .withColumn("Fourth", when(col("Fourth").isNull(), lit("*")).otherwise(col("Fourth")))\
        .withColumn("First3", (col("First2") + col("Third")))
