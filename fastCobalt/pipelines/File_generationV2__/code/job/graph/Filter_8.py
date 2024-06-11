from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_8(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        array_contains(
          array(
            lit("S of A calls"), 
            lit("Claims Status"), 
            lit("Claim Status Calls"), 
            lit("Benefits & Eligibilty Calls"), 
            lit("Precert Intake Calls"), 
            lit("Prov B&E Calls")
          ), 
          col("SUCat2Name").cast(StringType())
        )
    )
