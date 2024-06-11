from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_803(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("PCP Attributed", when((col("`PCP Attributed`").cast(StringType()) == lit("1")), lit("Y")).otherwise(lit("N")))\
        .withColumn(
          "SPIRA Elligible",
          when((col("`SPIRA Elligible`").cast(StringType()) == lit("CLNC")), lit("Y")).otherwise(lit("N"))
        )\
        .withColumn("Drug/Alcohol/Psych Related ED Visit", when(
          (
            ((col("`Alcohol Related ED`").cast(IntegerType()) == lit(1)) | (col("`Psych Related ED`").cast(IntegerType()) == lit(1)))
            | (col("`Drug Related ED`").cast(IntegerType()) == lit(1))
          ), 
          lit(1)
        )\
        .otherwise(lit(0)))
