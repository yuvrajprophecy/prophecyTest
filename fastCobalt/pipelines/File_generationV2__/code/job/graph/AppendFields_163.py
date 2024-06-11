from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AppendFields_163(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (lit(1) == lit(1).cast(IntegerType())), "inner")\
        .select(col("in0.`Lower Bound (95% CI)`").alias("Lower Bound (95% CI)"), col("in0.`Sum_Anticipated Lower`").alias("Sum_Anticipated Lower"), col("in1.Prct").alias("Prct"), col("in0.`Upper Bound (95% CI)`").alias("Upper Bound (95% CI)"), col("in0.`Sum_Anticipated Calls`").alias("Sum_Anticipated Calls"), col("in0.`Sum_Anticipated Upper`").alias("Sum_Anticipated Upper"), col("in0.Date").alias("Date"), col("in1.Time").alias("Time"), col("in0.`Anticipated Hours`").alias("Anticipated Hours"))
