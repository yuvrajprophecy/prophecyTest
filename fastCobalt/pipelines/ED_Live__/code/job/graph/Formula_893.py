from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_893(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Week",
          call_spark_fcn(
            "string_substring", 
            expr(
              "date_add(CLM_SVC_STRT_DT_SK, (CAST((-1 * CAST(CAST(dayofweek(CLM_SVC_STRT_DT_SK) AS INT) AS INT)) AS INT) + 1))"
            ), 
            lit(0), 
            lit(10)
          )
        )\
        .withColumn("Counter", lit(1))
