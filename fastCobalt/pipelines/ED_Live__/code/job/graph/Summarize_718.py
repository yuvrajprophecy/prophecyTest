from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_718(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SUB_ID"), col("Week"))

    return df1.agg(
        count_distinct(col("CLM_SVC_STRT_DT_SK"), lit(0)).alias("CountDistinct_CLM_SVC_STRT_DT_SK"), 
        sum(col("CLM_LN_ALW_AMT")).alias("Household ED Visit Alw Amt"), 
        sum(col("CLM_LN_TOT_ALW_AMT")).alias("Household ED Visit Tot Alw Amt")
    )
