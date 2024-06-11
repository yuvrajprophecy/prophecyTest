from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AppendFields_213(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (lit(1) == lit(1).cast(IntegerType())), "inner")\
        .select(col("in0.YMD").alias("YMD"), col("in0.dow").alias("dow"), col("in1.Time").alias("Time"), col("in1.Prct").alias("Prct"))
