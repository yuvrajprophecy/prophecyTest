from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_406(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "PROD_CAT",
        when(
            array_contains(array(lit("MA"), lit("MEDSUP"), lit("ACA")), col("PROD_CAT").cast(StringType())), 
            col("PROD_CAT")
          )\
          .when((col("FEP_FLAG").cast(IntegerType()) == lit(1)), lit("FEP"))\
          .otherwise(lit("COM"))
    )
