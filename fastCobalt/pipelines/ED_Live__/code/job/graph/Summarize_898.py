from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_898(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"))

    return df1.agg(
        sum(col("CountDistinct_CLM_ID")).alias("Sum_CountDistinct_CLM_ID"), 
        sum(col("PR")).alias("Sum_PR"), 
        sum(col("Sum_FCLTY_CLM_LOS_DAYS")).alias("Sum_Sum_FCLTY_CLM_LOS_DAYS"), 
        sum(col("OP")).alias("Sum_OP"), 
        sum(col("IP")).alias("Sum_IP")
    )
