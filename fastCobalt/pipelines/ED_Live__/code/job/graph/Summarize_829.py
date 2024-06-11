from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_829(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"))

    return df1.agg(
        sum(col("CLM_LN_TOT_ALW_AMT")).alias("Total Claim Spend"), 
        max(col("CLM_LN_TOT_ALW_AMT")).alias("Max Claim Line Spend")
    )
