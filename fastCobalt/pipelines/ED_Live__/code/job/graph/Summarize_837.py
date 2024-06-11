from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_837(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("Week"))

    return df1.agg(count_distinct(col("CLM_SVC_STRT_DT_SK"), lit(0)).alias("CountDistinct_CLM_SVC_STRT_DT_SK"))
