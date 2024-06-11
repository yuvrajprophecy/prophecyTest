from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AppendFields_175(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (lit(1) == lit(1).cast(IntegerType())), "inner")\
        .select(col("in1.Sum_Friday").alias("Sum_Friday"), col("in1.Sum_Wednesday").alias("Sum_Wednesday"), col("in0.Tuesday").alias("Tuesday"), col("in0.Wednesday").alias("Wednesday"), col("in1.Sum_Monday").alias("Sum_Monday"), col("in0.Monday").alias("Monday"), col("in0.Friday").alias("Friday"), col("in1.Sum_Thursday").alias("Sum_Thursday"), col("in0.Time").alias("Time"), col("in0.Thursday").alias("Thursday"), col("in1.Sum_Tuesday").alias("Sum_Tuesday"))
