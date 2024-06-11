from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Sort_1002(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("`ED Prediction Score`").asc(), col("`NONEMERGENT_COUNT_PAST_60 (OG)`").asc())
