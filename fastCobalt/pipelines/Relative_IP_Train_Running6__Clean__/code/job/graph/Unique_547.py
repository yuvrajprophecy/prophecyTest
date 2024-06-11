from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Unique_547(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy(
              "MBR_INDV_BE_KEY", 
              "DIAG_CD", 
              "DIAG_CD_DESC", 
              "CLM_SK", 
              "CLM_LN_SK", 
              "CLM_ID", 
              "CLM_LN_SEQ_NO", 
              "CLM_LN_SVC_STRT_DT_SK", 
              "CLM_LN_DIAG_SK", 
              "DIAG_CD_SK", 
              "CLM_LN_DIAG_CD_1_SK", 
              "CLM_LN_DIAG_CD_2_SK", 
              "CLM_LN_DIAG_CD_3_SK", 
              "DIAG_CD_TYP_CD", 
              "First2", 
              "Third", 
              "Fourth", 
              "YEARMONTH", 
              "Count"
            )\
            .orderBy(lit(1)))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
