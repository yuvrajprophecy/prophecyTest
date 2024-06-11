from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_4048(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(array_contains(array(lit("21"), lit("22"), lit("23")), col("CLM_LN_POS_CD").cast(StringType())))
