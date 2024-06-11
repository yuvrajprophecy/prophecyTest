from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_407(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (col("CLM_INPT_DT_SK") <= add_months(col("CLM_SVC_STRT_DT_SK"), 1))
          & (col("CLM_INPT_DT_SK") >= col("CLM_SVC_STRT_DT_SK"))
        )
    )
