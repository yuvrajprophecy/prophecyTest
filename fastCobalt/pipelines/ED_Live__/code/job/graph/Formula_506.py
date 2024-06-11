from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_506(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "GRP",
        when(
            array_contains(array(lit("African American"), lit("Caribbean Non-Hispanic")), col("GRP").cast(StringType())), 
            lit("Black")
          )\
          .when(
            array_contains(
              array(
                lit("Western European"), 
                lit("Eastern European"), 
                lit("Jewish"), 
                lit("Scandinavian"), 
                lit("Mediterranean")
              ), 
              col("GRP").cast(StringType())
            ), 
            lit("White")
          )\
          .when(
            array_contains(
              array(
                lit("South Asian"), 
                lit("Southeast Asian"), 
                lit("East Asian"), 
                lit("Central Asian"), 
                lit("Polynesian")
              ), 
              col("GRP").cast(StringType())
            ), 
            lit("Asian")
          )\
          .when(array_contains(array(lit("Hispanic")), col("GRP").cast(StringType())), lit("Hispanic"))\
          .when(
            array_contains(array(lit("Middle Eastern"), lit("Native American")), col("GRP").cast(StringType())), 
            lit("Other")
          )\
          .otherwise(lit("Unknown"))
    )
