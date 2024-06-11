from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Groupfemalecount(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("ACTVTY_YR_MO_SK"), col("GRP_ID"))

    return df1.agg(sum(col("`Indicator Female`")).alias("Sum_Indicator Female"))
