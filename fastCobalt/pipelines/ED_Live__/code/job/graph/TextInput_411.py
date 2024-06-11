from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_411(spark: SparkSession) -> DataFrame:
    
    colNames = ["Years"]
    data = [[2022], [2023], [2024]]
    rows = [Row(*row) for row in data]
    schema = StructType([
StructField("Years", IntegerType(), nullable = True)])
    out0 = spark.createDataFrame(rows, schema)

    return out0
