from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AppendFields_34(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (lit(1) == lit(1).cast(IntegerType())), "inner")\
        .select(col("in0.`RT Handle Time`").alias("RT Handle Time"), col("in1.yhat_upper").alias("yhat_upper"), col("in1.yhat").alias("yhat"), col("in0.`Call Type`").alias("Call Type"), col("in1.ds").alias("ds"), col("in1.Field_1").alias("Field_1"), col("in1.yhat_lower").alias("yhat_lower"), col("in0.`RT Percent of Calls`").alias("RT Percent of Calls"))
