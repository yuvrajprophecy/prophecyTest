from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_405(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          col("EXP_SUB_CAT_CD").contains(lit("EMERGENCY"))
          | array_contains(
            array(lit("0450"), lit("0451"), lit("0452"), lit("0456"), lit("0459"), lit("0981")), 
            col("RVNU_CD").cast(StringType())
          )
        )
    )
