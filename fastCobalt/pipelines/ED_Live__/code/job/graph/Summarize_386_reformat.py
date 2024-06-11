from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_386_reformat(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("noner"), 
        col("edcnnpa"), 
        col("Diagnosis"), 
        col("DIAG_CD"), 
        col("YMD"), 
        col("injury"), 
        col("psych"), 
        col("epct"), 
        col("Week"), 
        col("edcnpa"), 
        col("SUB_ID"), 
        col("MBR_INDV_BE_KEY"), 
        col("alcohol"), 
        col("drug"), 
        col("CLM_SVC_STRT_DT_SK")
    )
