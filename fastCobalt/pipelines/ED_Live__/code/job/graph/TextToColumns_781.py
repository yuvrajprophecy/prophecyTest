from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_781(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = textToColumns(in0, "ED Visited", 10, "ED Visited", ",")

    return out0
