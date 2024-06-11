from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Definespouseproductasretirebinary(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "Spouse's Product",
        when(
            array_contains(
              array(lit("BMADVP"), lit("BMADVH"), lit("MEDGAP"), lit("MEDSEL"), lit("TIP"), lit("CBLU65")), 
              col("`Spouse's Product`").cast(StringType())
            ), 
            lit(1)
          )\
          .otherwise(lit(0))
    )
