from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_892(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("Week"))

    return df1.agg(
        count_distinct(col("CLM_ID"), lit(0)).alias("CountDistinct_CLM_ID"), 
        sum(col("FCLTY_CLM_LOS_DAYS")).alias("Sum_FCLTY_CLM_LOS_DAYS")
    )
