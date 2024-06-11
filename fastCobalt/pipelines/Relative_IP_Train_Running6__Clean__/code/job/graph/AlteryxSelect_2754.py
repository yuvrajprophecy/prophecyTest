from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2754(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("YEARMONTH"), 
        col("LAB_RSLT_NUM_RSLT_VAL"), 
        col("LAB_RSLT_SVC_DT_SK"), 
        col("LAB_RSLT_CLS_NM"), 
        col("Max_A1C"), 
        col("LOINC_CD"), 
        col("Avg_LAB_RSLT_NUM_RSLT_VAL"), 
        col("LAB_RSLT_NORM_LOW_VAL"), 
        col("LAB_RSLT_DESC"), 
        col("MBR_INDV_BE_KEY"), 
        col("LAB_RSLT_NORM_HI_VAL"), 
        col("Min_A1C"), 
        col("ORDER_TST_NM"), 
        col("LOINC_CD_SK")
    )
