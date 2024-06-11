from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_41_right_UnionRightOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.YMD") == col("in1.YMD")), "rightouter")\
        .select(col("in0.TTLCalls").alias("TTLCalls"), col("in0.UnitName").alias("UnitName"), col("in0.ProdID").alias("ProdID"), col("in0.TTLHours").alias("TTLHours"), col("in1.YMD").alias("YMD"), col("in1.YMD").alias("Right_YMD"), col("in0.`Day of Week`").alias("Day of Week"), col("in0.UserID").alias("UserID"), col("in0.NetworkName").alias("NetworkName"), col("in0.ProductName").alias("ProductName"))
