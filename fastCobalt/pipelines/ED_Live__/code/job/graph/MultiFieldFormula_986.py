from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_986(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(
            (
              col("`NONEMERGENT_RATE (OG)`").isNull()
              | (length(col("`NONEMERGENT_RATE (OG)`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`NONEMERGENT_RATE (OG)`"))\
          .alias("NONEMERGENT_RATE (OG)"), 
        when(
            (
              col("`NONEMERGENT_RATE (Proposed)`").isNull()
              | (length(col("`NONEMERGENT_RATE (Proposed)`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`NONEMERGENT_RATE (Proposed)`"))\
          .alias("NONEMERGENT_RATE (Proposed)"), 
        when(
            (
              col("`NONEMERGENT_COUNT_PAST_60 (Proposed)`").isNull()
              | (length(col("`NONEMERGENT_COUNT_PAST_60 (Proposed)`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`NONEMERGENT_COUNT_PAST_60 (Proposed)`"))\
          .alias("NONEMERGENT_COUNT_PAST_60 (Proposed)"), 
        when(
            (
              col("`NONEMERGENT_COUNT_PAST_60 (OG)`").isNull()
              | (length(col("`NONEMERGENT_COUNT_PAST_60 (OG)`")).cast(IntegerType()) > lit(0))
            ), 
            lit(0)
          )\
          .otherwise(col("`NONEMERGENT_COUNT_PAST_60 (OG)`"))\
          .alias("NONEMERGENT_COUNT_PAST_60 (OG)"), 
        when((col("FREQUENT_FLYER").isNull() | (length(col("FREQUENT_FLYER")).cast(IntegerType()) > lit(0))), lit(0))\
          .otherwise(col("FREQUENT_FLYER"))\
          .alias("FREQUENT_FLYER"), 
        col("`ED Prediction Value`").alias("ED Prediction Value"), 
        col("LATEST_ED_DX"), 
        col("ER_COUNT"), 
        col("ER_COUNT_PAST_3_MONTHS"), 
        col("`Member Individual Business Entity Key`").alias("Member Individual Business Entity Key"), 
        col("ED_DSCHG_DT"), 
        col("`ED Prediction Score`").alias("ED Prediction Score"), 
        col("ER_COUNT_PAST_2_MONTHS")
    )
