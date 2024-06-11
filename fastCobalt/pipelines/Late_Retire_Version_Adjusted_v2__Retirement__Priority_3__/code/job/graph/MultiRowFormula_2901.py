from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2901(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("New Field", when((col("`mover indicator_lead1`").cast(IntegerType()) == lit(1)), lit(1)).otherwise(lit(0)))\
        .drop("mover indicator_lead1")
