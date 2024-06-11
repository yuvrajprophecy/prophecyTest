from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_961(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"))

    return df1.agg(
        max(col("CLM_SVC_STRT_DT_SK")).alias("ED_DSCHG_DT"), 
        count_distinct(col("CLM_ID"), lit(0)).alias("ER_COUNT"), 
        last(col("DIAG_CD_DESC")).alias("LATEST_ED_DX")
    )
