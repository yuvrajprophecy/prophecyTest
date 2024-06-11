from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_955(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("DIAG_CD"), 
        col("icd10"), 
        col("Diagnosis"), 
        col("edcnnpa"), 
        col("edcnpa"), 
        col("epct"), 
        col("noner"), 
        col("injury"), 
        col("psych"), 
        col("alcohol"), 
        col("drug")
    )
