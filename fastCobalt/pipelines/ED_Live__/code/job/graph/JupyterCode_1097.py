from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JupyterCode_1097(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    df1 = spark.createDataFrame([Row()])

    return df1