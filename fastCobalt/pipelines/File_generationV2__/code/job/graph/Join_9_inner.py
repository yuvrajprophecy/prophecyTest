from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_9_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.ProdID") == col("in1.ProdID")), "inner")\
        .select(col("in1.SUCat2Name").alias("SUCat2Name"), col("in1.ProdID").alias("Right_ProdID"), col("in0.ProdID").alias("ProdID"), col("in1.ProductName").alias("ProductName"), col("in0.YMD").alias("YMD"), col("in1.SUCat1Name").alias("SUCat1Name"), col("in0.Sum_TTLCalls").alias("Sum_TTLCalls"), col("in0.Sum_TTLHours").alias("Sum_TTLHours"), col("in0.NetworkName").alias("NetworkName"))
