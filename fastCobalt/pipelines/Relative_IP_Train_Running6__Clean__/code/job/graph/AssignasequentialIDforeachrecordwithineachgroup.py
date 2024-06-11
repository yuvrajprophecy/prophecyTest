from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AssignasequentialIDforeachrecordwithineachgroup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        RecordNum_lag1 = 0.0

        for row in iterator:
            RecordNum_new = int(RecordNum_lag1) + 1
            RecordNum_lag1 = RecordNum_new
            newRow = list(row)
            newRow.append(RecordNum_new)
            yield newRow

    resultRDD = in0.repartition(col("MBR_INDV_BE_KEY")).sortWithinPartitions(col("MBR_INDV_BE_KEY")).rdd.mapPartitions(
        calculate
    )
    newSchema = StructType([field for field in in0.schema.fields if field.name != "RecordNum"])
    newSchema.add("RecordNum", DoubleType())
    out0 = spark.createDataFrame(resultRDD, newSchema)

    return out0
