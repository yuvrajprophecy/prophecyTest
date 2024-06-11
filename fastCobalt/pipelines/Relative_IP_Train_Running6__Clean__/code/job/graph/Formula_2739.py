from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_2739(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "GFR_Stage",
          when((col("LowestGFR").cast(IntegerType()) < lit(15)), lit("G5"))\
            .when((col("LowestGFR").cast(IntegerType()) <= lit(29)), lit("G4"))\
            .when((col("LowestGFR").cast(IntegerType()) <= lit(44)), lit("G3b"))\
            .when((col("LowestGFR").cast(IntegerType()) <= lit(59)), lit("G3a"))\
            .when((col("LowestGFR").cast(IntegerType()) <= lit(90)), lit("G2"))\
            .when((col("LowestGFR").cast(IntegerType()) >= lit(90)), lit("G1"))\
            .otherwise(lit("Not Recorded"))
        )\
        .withColumn("Albuminuria_Category", when(
          ((col("mmol").cast(IntegerType()) == lit(0)) & (col("HighestAlbumin").cast(IntegerType()) < lit(3))), 
          lit("A1")
        )\
        .when(
          ((col("mmol").cast(IntegerType()) == lit(0)) & (col("HighestAlbumin").cast(IntegerType()) < lit(30))), 
          lit("A2")
        )\
        .when(
          (
            ((col("mmol").cast(IntegerType()) == lit(0)) & (col("HighestAlbumin").cast(IntegerType()) >= lit(30)))
            & (col("HighestAlbumin").cast(IntegerType()) != lit(9999999))
          ), 
          lit("A3")
        )\
        .when(
          ((col("mmol").cast(IntegerType()) == lit(1)) & (col("HighestAlbumin").cast(IntegerType()) < lit(30))), 
          lit("A1")
        )\
        .when(
          ((col("mmol").cast(IntegerType()) == lit(1)) & (col("HighestAlbumin").cast(IntegerType()) <= lit(299))), 
          lit("A2")
        )\
        .when(
          (
            ((col("mmol").cast(IntegerType()) == lit(1)) & (col("HighestAlbumin").cast(IntegerType()) >= lit(300)))
            & (col("HighestAlbumin").cast(IntegerType()) != lit(9999999))
          ), 
          lit("A3")
        )\
        .otherwise(lit("Not Recorded")))
