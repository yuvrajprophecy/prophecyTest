from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Malefemalepercentages(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Male Percentage",
          (col("`Sum_Indicator Male`") / (col("`Sum_Indicator Male`") + col("`Sum_Indicator Female`")))
        )\
        .withColumn("Female Percentage", (col("`Sum_Indicator Female`") / (col("`Sum_Indicator Male`") + col("`Sum_Indicator Female`"))))
