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
delimitter3 = ''

def registerUDFs(spark: SparkSession):
    spark.udf.register("udfConcat", udfConcat)
    spark.udf.register("udfConcat2", udfConcat2)
    spark.udf.register("udfConcat3", udfConcat3)

def udfConcatGenerator():
    delimitter1 = ''

    @udf(returnType = StringType())
    def func(value: str, value2: str):
        return value + delimitter1 + value2

    return func

udfConcat = udfConcatGenerator()

def udfConcat2Generator():
    delimitter2 = ''

    @udf(returnType = StringType())
    def func(value: str, value2: str):
        return value + delimitter2 + value2

    return func

udfConcat2 = udfConcat2Generator()

def udfConcat3Generator():
    delimitter3 = ''

    @udf(returnType = StringType())
    def func(value: str, value2: str):
        return value + delimitter3 + value2

    return func

udfConcat3 = udfConcat3Generator()
