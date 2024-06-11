from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_4049(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "CLM_LN_POS_CD",
          when((col("CLM_LN_POS_CD").cast(StringType()) == lit("21")), lit("Inpatient"))\
            .when((col("CLM_LN_POS_CD").cast(StringType()) == lit("22")), lit("Outpatient"))\
            .otherwise(lit("Emergency Room"))
        )\
        .withColumn("Counter", lit(1))
