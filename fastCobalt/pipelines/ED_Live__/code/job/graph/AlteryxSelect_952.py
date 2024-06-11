from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_952(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("edcnpa"), 
        col("psych"), 
        col("alcohol"), 
        col("DIAG_CD"), 
        col("noner"), 
        col("icd10"), 
        col("injury"), 
        col("drug"), 
        col("epct"), 
        col("edcnnpa")
    )
