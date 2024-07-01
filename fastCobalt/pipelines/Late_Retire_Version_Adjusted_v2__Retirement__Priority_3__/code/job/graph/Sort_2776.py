from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Sort_2776(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("`Member BE Key`").asc(), col("ACTVTY_YR_MO_SK").asc())