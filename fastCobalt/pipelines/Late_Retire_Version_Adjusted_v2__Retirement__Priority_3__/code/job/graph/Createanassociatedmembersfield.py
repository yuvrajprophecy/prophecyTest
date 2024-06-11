from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Createanassociatedmembersfield(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        Associated_Members_lag1 = 0.0

        for row in iterator:
            MBR_INDV_BE_KEY = row["MBR_INDV_BE_KEY"]
            Associated_Members_new = concat(concat(Associated_Members_lag1, ""), MBR_INDV_BE_KEY)
            Associated_Members_lag1 = Associated_Members_new
            newRow = list(row)
            newRow.append(Associated_Members_new)
            yield newRow

    resultRDD = in0.repartition(col("SUB_SK")).sortWithinPartitions(col("SUB_SK")).rdd.mapPartitions(calculate)
    newSchema = StructType([field for field in in0.schema.fields if field.name != "Associated_Members"])
    newSchema.add("Associated_Members", DoubleType())
    out0 = spark.createDataFrame(resultRDD, newSchema)

    return out0
