from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Sort_3986(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(
        col("Member").asc(), 
        col("FIRST_DT_OF_MO").asc(), 
        col("Spouse").asc(), 
        col("`Spouse's Product`").desc()
    )
