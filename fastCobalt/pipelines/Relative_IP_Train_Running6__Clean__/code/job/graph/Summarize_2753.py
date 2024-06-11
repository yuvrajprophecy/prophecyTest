from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_2753(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("LAB_RSLT_SVC_DT_SK"))

    return df1.agg(max(col("GFR/EGFR")).alias("GFR/EGFR"), min(col("LAB_RSLT_NUM_RSLT_VAL")).alias("LowestGFR"))
