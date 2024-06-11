from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_387(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Diagnosis"), 
        col("psych"), 
        col("alcohol"), 
        col("DIAG_CD"), 
        col("Week"), 
        col("YMD"), 
        col("injury"), 
        col("drug"), 
        col("MBR_INDV_BE_KEY"), 
        col("SUB_ID"), 
        col("CLM_SVC_STRT_DT_SK"), 
        col("noner").alias("ER Severity")
    )
