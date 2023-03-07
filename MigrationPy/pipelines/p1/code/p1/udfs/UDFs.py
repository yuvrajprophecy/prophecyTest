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
delimitter1 = ''

def registerUDFs(spark: SparkSession):
    spark.udf.register("udfConcat", udfConcat)

def udfConcatGenerator():
    delimitter1 = ''

    @udf(returnType = StringType())
    def func(value: str, value2: str):
        return value + delimitter1 + value2

    return func

udfConcat = udfConcatGenerator()
