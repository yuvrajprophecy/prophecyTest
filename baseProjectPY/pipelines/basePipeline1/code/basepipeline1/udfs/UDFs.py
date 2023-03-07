from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)
y = 10

def registerUDFs(spark: SparkSession):
    spark.udf.register("udfConcat", udfConcat)
    spark.udf.register("udfConcatold", udfConcatold)

def udfConcatGenerator():
    delimitter = ' '

    @udf(returnType = StringType())
    def func(value: str, value2: str):
        return value + delimitter + value2

    return func

udfConcat = udfConcatGenerator()

@udf(returnType = StringType())
def udfConcatold(value: str, value2: str):
    return value + value2
