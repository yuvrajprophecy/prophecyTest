from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Sample_204(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("row_number", row_number().over(Window.partitionBy().orderBy(lit(1).asc())))
