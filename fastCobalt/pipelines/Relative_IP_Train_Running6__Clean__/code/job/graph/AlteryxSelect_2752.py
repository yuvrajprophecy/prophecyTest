from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2752(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("MBR_INDV_BE_KEY"), col("LAB_RSLT_SVC_DT_SK"), col("GFR/EGFR"), col("LowestGFR"))
