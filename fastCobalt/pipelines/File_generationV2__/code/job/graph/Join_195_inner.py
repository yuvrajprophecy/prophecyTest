from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_195_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Date") == col("in1.ds")), "inner")\
        .select(col("in0.`Lower Bound (95% CI)`").alias("Lower Bound (95% CI)"), col("in0.`Sum_Anticipated Lower`").alias("Sum_Anticipated Lower"), col("in0.`Upper Bound (95% CI)`").alias("Upper Bound (95% CI)"), col("in0.`Sum_Anticipated Calls`").alias("Sum_Anticipated Calls"), col("in0.`Sum_Anticipated Upper`").alias("Sum_Anticipated Upper"), col("in0.Date").alias("Date"), col("in0.`Anticipated Hours`").alias("Anticipated Hours"))
