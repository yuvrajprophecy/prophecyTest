from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_767(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "PCMH_flag",
          when(
              (
                ((col("`Medical Home`").cast(StringType()) != lit("NA")) & ~ col("`Medical Home`").isNull())
                & ~ col("`Medical Home`").contains(lit("OOA"))
              ), 
              lit("Y")
            )\
            .otherwise(lit("N"))
        )\
        .withColumn(
          "Medical Home",
          when(
              (
                ((col("`Medical Home`").cast(StringType()) == lit("NA")) | col("`Medical Home`").contains(lit("OOA")))
                | col("`Medical Home`").isNull()
              ), 
              lit("None Attributed")
            )\
            .otherwise(col("`Medical Home`"))
        )\
        .withColumn("MED_HOME_ID", when(
          (
            ((col("MED_HOME_ID").cast(StringType()) == lit("0")) | col("MED_HOME_ID").isNull())
            | (col("MED_HOME_ID").cast(StringType()) == lit("NA"))
          ), 
          lit("NA")
        )\
        .otherwise(col("MED_HOME_ID")))
