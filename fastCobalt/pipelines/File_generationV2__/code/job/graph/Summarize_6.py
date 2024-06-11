from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_6(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("YMD"), col("NetworkName"), col("ProdID"))

    return df1.agg(sum(col("TTLCalls")).alias("Sum_TTLCalls"), sum(col("TTLHours")).alias("Sum_TTLHours"))
