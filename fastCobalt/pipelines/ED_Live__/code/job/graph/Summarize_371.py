from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_371(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("Week"))

    return df1.agg(sum(col("`ED Visits`")).alias("Sum_ED Visits"))
