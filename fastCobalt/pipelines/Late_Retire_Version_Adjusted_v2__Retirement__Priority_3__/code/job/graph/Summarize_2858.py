from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_2858(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("ACTVTY_YR_MO_SK"), col("GRP_ID"))

    return df1.agg(count_distinct(col("MBR_INDV_BE_KEY"), lit(0)).alias("CountDistinct_MBR_INDV_BE_KEY"))
