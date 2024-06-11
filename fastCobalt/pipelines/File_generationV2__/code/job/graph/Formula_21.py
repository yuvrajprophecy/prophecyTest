from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_21(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "1",
          when((length(col("`1`")).cast(IntegerType()) == lit(1)), concat(lit("0"), col("`1`"))).otherwise(col("`1`"))
        )\
        .withColumn(
          "2",
          when((length(col("`2`")).cast(IntegerType()) == lit(1)), concat(lit("0"), col("`2`"))).otherwise(col("`2`"))
        )\
        .withColumn("YMD", concat(concat(concat(concat(col("`3`"), lit("-")), col("`1`")), lit("-")), col("`2`")))\
        .withColumn("Day of Week", date_format(col("YMD"), "EEEE"))
