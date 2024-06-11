from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_11(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "Call Type",
        when(
            array_contains(
              array(lit("Prov B&E Calls"), lit("Benefits & Eligibilty Calls")), 
              col("SUCat2Name").cast(StringType())
            ), 
            lit("BnE")
          )\
          .when(array_contains(array(lit("S of A Calls")), col("SUCat2Name").cast(StringType())), lit("SoA"))\
          .when(array_contains(array(lit("Precert Intake Calls")), col("SUCat2Name").cast(StringType())), lit("Precert"))\
          .when(
            array_contains(array(lit("Claims Status"), lit("Claim Status Calls")), col("SUCat2Name").cast(StringType())), 
            lit("Claim")
          )\
          .otherwise(lit("Other (Idle)"))
    )
