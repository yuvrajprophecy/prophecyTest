from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_3247(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SUB_UNIQ_KEY"), col("FIRST_DT_OF_MO"))

    return df1.agg(min(col("`Dependent Age`")).alias("Min_Dependent Age"))
