from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_2775(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("`Member BE Key`").alias("Member BE Key"), col("ACTVTY_YR_MO_SK"))

    return df1.agg(
        count_distinct(col("`Household Member BE Key`"), lit(0)).alias("CountDistinct_Household Member BE Key")
    )
