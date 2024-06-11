from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_716(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("Week"))

    return df1.agg(
        count_distinct(col("CLM_SVC_STRT_DT_SK"), lit(0)).alias("CountDistinct_CLM_SVC_STRT_DT_SK"), 
        sum(col("CLM_LN_ALW_AMT")).alias("ED Visit Alw Amt"), 
        sum(col("CLM_LN_TOT_ALW_AMT")).alias("ED Visit Tot Alw Amt"), 
        concat_ws(",", collect_list(col("PROV_ID"))).alias("ED Visited")
    )
