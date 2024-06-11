from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_977(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (col("CLM_SVC_STRT_DT_SK") > expr("date_add(current_date(), -60)"))
          & (col("CLM_SVC_STRT_DT_SK") < current_date())
        )
    )
