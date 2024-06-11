from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_988(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`ED Prediction Score`").alias("ED Prediction Score"), 
        col("`NONEMERGENT_RATE (OG)`").alias("NONEMERGENT_RATE (OG)")
    )

    return df1.agg(
        count_distinct(col("`Member Individual Business Entity Key`"), lit(0))\
          .alias("CountDistinct_Member Individual Business Entity Key")
    )
