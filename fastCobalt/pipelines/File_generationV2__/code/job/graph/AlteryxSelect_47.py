from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_47(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("Field_1"), col("ds"), col("yhat"), col("yhat_lower"), col("yhat_upper"))
