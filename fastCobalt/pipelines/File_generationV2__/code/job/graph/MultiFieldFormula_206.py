from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_206(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(col("`Anticipated Hours`").isNull(), lit(0))\
          .otherwise(col("`Anticipated Hours`"))\
          .alias("Anticipated Hours"), 
        when(col("`Upper Bound (95% CI)`").isNull(), lit(0))\
          .otherwise(col("`Upper Bound (95% CI)`"))\
          .alias("Upper Bound (95% CI)"), 
        when(col("`Lower Bound (95% CI)`").isNull(), lit(0))\
          .otherwise(col("`Lower Bound (95% CI)`"))\
          .alias("Lower Bound (95% CI)"), 
        when(col("Prct").isNull(), lit(0)).otherwise(col("Prct")).alias("Prct"), 
        when(col("Calls").isNull(), lit(0)).otherwise(col("Calls")).alias("Calls"), 
        when(col("`Calls Lower`").isNull(), lit(0)).otherwise(col("`Calls Lower`")).alias("Calls Lower"), 
        when(col("`Calls Upper`").isNull(), lit(0)).otherwise(col("`Calls Upper`")).alias("Calls Upper"), 
        when(col("`Time Distributed Calls`").isNull(), lit(0))\
          .otherwise(col("`Time Distributed Calls`"))\
          .alias("Time Distributed Calls"), 
        when(col("`Time Distributed Calls Lower`").isNull(), lit(0))\
          .otherwise(col("`Time Distributed Calls Lower`"))\
          .alias("Time Distributed Calls Lower"), 
        when(col("`Time Distributed Calls Upper`").isNull(), lit(0))\
          .otherwise(col("`Time Distributed Calls Upper`"))\
          .alias("Time Distributed Calls Upper"), 
        when(col("`Time Distributed Hours`").isNull(), lit(0))\
          .otherwise(col("`Time Distributed Hours`"))\
          .alias("Time Distributed Hours"), 
        when(col("`Time Distributed Hours Lower`").isNull(), lit(0))\
          .otherwise(col("`Time Distributed Hours Lower`"))\
          .alias("Time Distributed Hours Lower"), 
        when(col("`Time Distributed Hours Upper`").isNull(), lit(0))\
          .otherwise(col("`Time Distributed Hours Upper`"))\
          .alias("Time Distributed Hours Upper"), 
        col("Date"), 
        col("Time")
    )
